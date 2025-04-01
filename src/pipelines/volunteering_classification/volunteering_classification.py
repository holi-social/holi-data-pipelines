import logging

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from dataclasses import dataclass, asdict
from typing import List, cast

from google.cloud.bigquery import Client
from prefect.states import State, Completed
from prefect_gcp import GcpCredentials
from tenacity import RetryError
import vertexai

from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.common.variables import variable_value
from src.pipelines.bigquery import bigquery_datalake, bigquery_datapool
from src.pipelines.volunteering_classification.similarity_classification import SimilarityClassifier
from src.pipelines.volunteering_classification.topics_skills_classification_v2 import (
    Item,
    TopicsSkillsClassifierV2,
    TopicsSkillsClassificationV2,
)

logger = PipelineLogger("volunteering_classification")

GCLOUD_PROJECT_ID = "holi-shared"
GCLOUD_LOCATION = "europe-north1"

ENVIRONMENT = cast(str, variable_value("prefect_environment"))

TABLE_NAME_CLASSIFICATIONS = f"{bigquery_datalake.project}.{bigquery_datalake.dataset}.classifications"
VIEW_NAME_WITHOUT_CLASSIFICATIONS = (
    f"{bigquery_datapool.project}.{bigquery_datapool.dataset}.volunteering_voltastics_without_classifications"
)

STOP_AFTER_BATCH = 1  # TODO set to 0 after development
BATCH_SIZE = 10  # TODO increase after development


@dataclass
class ClassificationResult:
    type: str
    id: str
    hash: str
    topics_skills_embedding_v2: List[float]
    topics_v2: List[str]
    skills_v2: List[str]
    similarity_embedding: List[float]


class VolunteeringClassifier:
    topics_skills_v2_classifier: TopicsSkillsClassifierV2
    similarity_classifier: SimilarityClassifier
    bqclient: Client
    bqclient_writing: Client

    def __init__(self) -> None:
        vertexai.init(project=GCLOUD_PROJECT_ID, location=GCLOUD_LOCATION)
        self.topics_skills_v2_classifier = TopicsSkillsClassifierV2()
        self.similarity_classifier = SimilarityClassifier()
        gcp_credentials = GcpCredentials(project=GCLOUD_PROJECT_ID)
        self.bqclient = gcp_credentials.get_bigquery_client(project=GCLOUD_PROJECT_ID, location=GCLOUD_LOCATION)
        self.bqclient_writing = gcp_credentials.get_bigquery_client(
            project=GCLOUD_PROJECT_ID, location=GCLOUD_LOCATION
        )

    def _classify_item(self, batch_nr: int, item: Item) -> ClassificationResult | None:
        try:
            if not item.get("topics_skills_embedding_v2"):
                logger.debug(f"[{item.id}] no existing topics&skills V2 classification found, running classifier")
                topics_skills_classification = self.topics_skills_v2_classifier.get_classification(item)
            else:
                logger.debug(f"[{item.id}] using existing topics&skills V2 classification")
                topics_skills_classification = TopicsSkillsClassificationV2(
                    topics_skills_embedding_v2=item.topics_skills_embedding_v2,
                    topics_v2=item.topics_v2,
                    skills_v2=item.skills_v2,
                )
            if not item.get("similarity_embedding"):
                logger.debug(f"[{item.id}] no existing similarity classification found, running classifier")
                similarity_embedding = self.similarity_classifier.classify(item)
            else:
                logger.debug(f"[{item.id}] using existing similarity classification")
                similarity_embedding = item.similarity_embedding
            return ClassificationResult(
                type="volunteering",
                id=item.id,
                hash=item.hash,
                topics_skills_embedding_v2=topics_skills_classification.topics_skills_embedding_v2,
                topics_v2=topics_skills_classification.topics_v2,
                skills_v2=topics_skills_classification.skills_v2,
                similarity_embedding=similarity_embedding,
            )
        except RetryError as e:
            last_attempt = e.last_attempt
            last_response = last_attempt.result() if last_attempt.result() else "No response"
            logger.error(
                f"Batch {batch_nr} - item {item.id} - failed to classify. Last model response: {last_response}.",
                exc_info=e,
            )
            return None
        except Exception as e:
            logger.error(f"Batch {batch_nr} - item {item.id} - failed to classify", exc_info=e)
            return None

    def _classify_and_store_item(
        self,
        batch_nr: int,
        item: Item,
    ) -> None:
        logger.debug(f"Batch {batch_nr} - item id={item.id} - classifying item")
        try:
            classification = self._classify_item(batch_nr, item)
            if classification is None:
                logger.debug(f"Batch {batch_nr} - item id={item.id} - failed to classify item, skipping")
                return
            errors = self.bqclient_writing.insert_rows_json(TABLE_NAME_CLASSIFICATIONS, [asdict(classification)])
            logger.debug(f"Batch {batch_nr} - item id={item.id} - written to BigQuery")
            if len(errors) > 0:
                raise Exception(f"Failed to insert classification {classification}: {errors}")
        except Exception as e:
            logger.error(
                f"Error while classifying item id={item.id}, skipping, Error: %s",
                str(e),
                exc_info=e,
            )

    def compute_classifications(self) -> None:
        logger.info(f"Starting classification for volunteering in {ENVIRONMENT}")
        pool = ThreadPoolExecutor()
        batch_nr = 0

        while not STOP_AFTER_BATCH or batch_nr < STOP_AFTER_BATCH:
            logger.info(f"Batch {batch_nr} - starting batch (of size {BATCH_SIZE})")

            total_items_count_result = self.bqclient.query(
                f"SELECT count(*) FROM `{VIEW_NAME_WITHOUT_CLASSIFICATIONS}`"  # noqa: S608
            ).result()
            total_items_count = next(total_items_count_result)[0]
            logger.info(f"Batch {batch_nr} - found {total_items_count} items to classify")

            # exit early if there are no items to classify
            if total_items_count == 0:
                logger.info(f"Batch {batch_nr} - all items classified")
                break  # exit while loop

            batch_items = self.bqclient.query(
                f"SELECT * FROM `{VIEW_NAME_WITHOUT_CLASSIFICATIONS}` LIMIT {BATCH_SIZE}"  # noqa: S608
            ).result()
            logger.info(f"Batch {batch_nr} - starting classification of {batch_items.total_rows} items")

            futures = [pool.submit(self._classify_and_store_item, batch_nr, item) for item in batch_items]
            wait(futures, return_when=ALL_COMPLETED)

            logger.info(f"Batch {batch_nr} - finished classification of {batch_items.total_rows} items")
            batch_nr += 1

        logger.info("Finished classification of volunteering")


@monitored_flow
def volunteering_classification() -> State:
    classifier = VolunteeringClassifier()
    classifier.compute_classifications()
    return Completed()


if __name__ == "__main__":
    logging.getLogger("volunteering_classification").setLevel(logging.DEBUG)
    logging.getLogger("volunteering_topics_skills_classification_v2").setLevel(logging.DEBUG)
    logging.getLogger("similarity_classification").setLevel(logging.DEBUG)
    volunteering_classification()
