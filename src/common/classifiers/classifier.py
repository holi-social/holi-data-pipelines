from abc import ABC
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait
from typing import cast, Optional

import vertexai
from google.cloud.bigquery import Client
from prefect_gcp import GcpCredentials
from tenacity import RetryError

from src.common.classifiers.classification_item import ClassificationItem
from src.common.classifiers.classification_result import ClassificationResult
from src.common.classifiers.topics_skills_classifier_v2 import TopicsSkillsClassificationV2, TopicsSkillsClassifierV2
from src.common.embedders.similarity_embedder import SimilarityEmbedder
from src.common.logging.PipelineLogger import PipelineLogger
from src.pipelines.bigquery import table_classifications

# TODO make configurable
GCLOUD_PROJECT_ID = "holi-shared"
GCLOUD_LOCATION = "europe-north1"

DEFAULT_BATCH_SIZE = 100


class Classifier(ABC):
    topics_skills_v2_classifier: TopicsSkillsClassifierV2
    similarity_embedder: SimilarityEmbedder
    bqclient: Client
    bqclient_writing: Client
    item_type: str
    view_name_without_classifications: str
    logger: PipelineLogger
    batch_size: int
    stop_after_batch_nr: Optional[int]

    def __init__(
        self,
        topics_skills_classifier_v2: TopicsSkillsClassifierV2,
        similarity_embedder: SimilarityEmbedder,
        item_type: str,
        view_name_without_classifications: str,
        logger: PipelineLogger,
        batch_size: int = DEFAULT_BATCH_SIZE,
        stop_after_batch_nr: Optional[int] = None,
    ):
        vertexai.init(project=GCLOUD_PROJECT_ID, location=GCLOUD_LOCATION)
        self.topics_skills_v2_classifier = topics_skills_classifier_v2
        self.similarity_embedder = similarity_embedder
        gcp_credentials = GcpCredentials(project=GCLOUD_PROJECT_ID)
        self.bqclient = gcp_credentials.get_bigquery_client(project=GCLOUD_PROJECT_ID, location=GCLOUD_LOCATION)
        self.bqclient_writing = gcp_credentials.get_bigquery_client(
            project=GCLOUD_PROJECT_ID, location=GCLOUD_LOCATION
        )
        self.item_type = item_type
        self.view_name_without_classifications = view_name_without_classifications
        self.logger = logger
        self.batch_size = batch_size
        self.stop_after_batch_nr = stop_after_batch_nr
        # TODO implement context manager API to cleanly handle resource shutdown

    def _classify_item(self, batch_nr: int, item: ClassificationItem) -> ClassificationResult | None:
        try:
            if not item.get("topics_skills_embedding_v2"):
                self.logger.debug(f"[{item.id}] no existing topics&skills V2 classification found, running classifier")
                topics_skills_classification = self.topics_skills_v2_classifier.get_classification(item)
            else:
                self.logger.debug(f"[{item.id}] using existing topics&skills V2 classification")
                topics_skills_classification = TopicsSkillsClassificationV2(
                    topics_skills_embedding_v2=item.topics_skills_embedding_v2,
                    topics_v2=item.topics_v2 or [],  # always set when topics_skills_embedding_v2 exists
                    skills_v2=item.skills_v2 or [],  # always set when topics_skills_embedding_v2 exists
                )
            if not item.get("similarity_embedding"):
                self.logger.debug(f"[{item.id}] no existing similarity classification found, running classifier")
                similarity_embedding = self.similarity_embedder.embed(item)
            else:
                self.logger.debug(f"[{item.id}] using existing similarity classification")
                similarity_embedding = item.similarity_embedding
            return ClassificationResult(
                type=self.item_type,
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
            self.logger.error(
                f"Batch {batch_nr} - item {item.id} - failed to classify. Last model response: {last_response}.",
                exc_info=e,
            )
            return None
        except Exception as e:
            self.logger.error(f"Batch {batch_nr} - item {item.id} - failed to classify", exc_info=e)
            return None

    def _classify_and_store_item(
        self,
        batch_nr: int,
        item: ClassificationItem,
    ) -> None:
        self.logger.info(f"Batch {batch_nr} - item id={item.id} - classifying item {item}")  # TODO revert
        try:
            classification = self._classify_item(batch_nr, item)
            if classification is None:
                self.logger.debug(f"Batch {batch_nr} - item id={item.id} - failed to classify item, skipping")
                return
            errors = self.bqclient_writing.insert_rows_json(table_classifications(), [classification.to_dict()])
            self.logger.debug(f"Batch {batch_nr} - item id={item.id} - written to BigQuery")
            if len(errors) > 0:
                raise Exception(f"Failed to insert classification {classification}: {errors}")
        except Exception as e:
            self.logger.error(
                f"Error while classifying item id={item.id}, skipping, Error: %s",
                str(e),
                exc_info=e,
            )

    def compute_classifications(self) -> None:
        self.logger.info(f"Starting classification for {self.item_type}")
        pool = ThreadPoolExecutor()
        batch_nr = 0

        while not self.stop_after_batch_nr or batch_nr < self.stop_after_batch_nr:
            self.logger.info(f"Batch {batch_nr} - starting batch (of size {self.batch_size})")

            total_items_count_result = self.bqclient.query(
                f"SELECT count(*) FROM `{self.view_name_without_classifications}`"  # noqa: S608
            ).result()
            total_items_count = next(total_items_count_result)[0]
            if total_items_count == 0:
                self.logger.info(f"Batch {batch_nr} - all items classified")
                break  # exit while loop
            else:
                self.logger.info(f"Batch {batch_nr} - {total_items_count} items remain to be classified")

            batch_items_query = f"SELECT * FROM `{self.view_name_without_classifications}` LIMIT {self.batch_size}"  # noqa: S608
            self.logger.info(f"Batch {batch_nr} - querying `{batch_items_query}`")
            batch_items = self.bqclient.query(batch_items_query).result()
            self.logger.info(f"Batch {batch_nr} - starting classification of {batch_items.total_rows} items")

            futures = [
                pool.submit(self._classify_and_store_item, batch_nr, cast(ClassificationItem, item))
                for item in batch_items
            ]
            wait(futures, return_when=ALL_COMPLETED)

            self.logger.info(f"Batch {batch_nr} - finished classification of {batch_items.total_rows} items")
            batch_nr += 1

        self.logger.info(f"Finished classification of {self.item_type}")
