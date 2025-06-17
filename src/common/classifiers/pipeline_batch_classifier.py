from abc import abstractmethod
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait
from logging import Logger
from typing import Optional, Generic, Any, Callable

from prefect_gcp import GcpCredentials

from shared.classifiers.classifier import Classifier
from shared.classifiers.classification_input import ClassificationInputT

import vertexai
from shared.classifiers.topics_skills_classifier_v2 import TopicsSkillsClassifierV2
from shared.classifiers.commitment_duration_classifier import CommitmentDurationClassifier
from shared.embedders.similarity_embedder import SimilarityEmbedder

DEFAULT_BATCH_SIZE = 100
GCLOUD_PROJECT_ID = "holi-shared"
GCLOUD_LOCATION = "europe-north1"


class PipelineBatchClassifier(Generic[ClassificationInputT], Classifier[ClassificationInputT]):
    batch_size: int
    stop_after_batch_nr: Optional[int]
    view_name_without_classifications: str

    def __init__(
        self,
        topics_skills_classifier_v2: TopicsSkillsClassifierV2[ClassificationInputT],
        similarity_embedder: SimilarityEmbedder[ClassificationInputT],
        commitment_label_extractor: Optional[Callable[[ClassificationInputT], Optional[str]]],
        location_type_extractor: Optional[Callable[[ClassificationInputT], Optional[str]]],
        commitment_duration_classifier: Optional[CommitmentDurationClassifier[ClassificationInputT]],
        item_type: str,
        table_name_classifications: str,
        view_name_without_classifications: str,
        logger: Logger,
        batch_size: int | None = DEFAULT_BATCH_SIZE,
        stop_after_batch_nr: int | None = None,
    ):
        super().__init__(
            topics_skills_classifier_v2=topics_skills_classifier_v2,
            similarity_embedder=similarity_embedder,
            commitment_label_extractor=commitment_label_extractor,
            location_type_extractor=location_type_extractor,
            commitment_duration_classifier=commitment_duration_classifier,
            item_type=item_type,
            table_name_classifications=table_name_classifications,
            logger=logger,
            bqclient=GcpCredentials(project=GCLOUD_PROJECT_ID).get_bigquery_client(
                project=GCLOUD_PROJECT_ID, location=GCLOUD_LOCATION
            ),
        )
        vertexai.init(project=GCLOUD_PROJECT_ID, location=GCLOUD_LOCATION)
        self.batch_size = batch_size if batch_size else DEFAULT_BATCH_SIZE
        self.stop_after_batch_nr = stop_after_batch_nr
        self.view_name_without_classifications = view_name_without_classifications

    @abstractmethod
    def _to_classification_input(self, item: Any) -> ClassificationInputT:
        pass

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
                pool.submit(self.classify_item_and_cache_classification, self._to_classification_input(item))
                for item in batch_items
            ]
            wait(futures, return_when=ALL_COMPLETED)

            self.logger.info(f"Batch {batch_nr} - finished classification of {batch_items.total_rows} items")
            batch_nr += 1

        self.logger.info(f"Finished classification of {self.item_type}")
