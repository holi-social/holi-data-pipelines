from abc import ABC
from logging import Logger

from google.cloud.bigquery import Client as BigQueryClient
from tenacity import RetryError

from shared.classifiers.classification_item import ClassificationItem
from shared.classifiers.classification_result import ClassificationResult
from shared.classifiers.topics_skills_classifier_v2 import TopicsSkillsClassificationV2, TopicsSkillsClassifierV2
from shared.embedders.similarity_embedder import SimilarityEmbedder


class Classifier(ABC):
    topics_skills_v2_classifier: TopicsSkillsClassifierV2
    similarity_embedder: SimilarityEmbedder
    bqclient: BigQueryClient
    item_type: str
    table_name_classifications: str
    logger: Logger

    def __init__(
        self,
        topics_skills_classifier_v2: TopicsSkillsClassifierV2,
        similarity_embedder: SimilarityEmbedder,
        item_type: str,
        table_name_classifications: str,
        logger: Logger,
        bqclient: BigQueryClient,
    ):
        self.topics_skills_v2_classifier = topics_skills_classifier_v2
        self.similarity_embedder = similarity_embedder
        self.bqclient = bqclient
        self.item_type = item_type
        self.table_name_classifications = table_name_classifications
        self.logger = logger
        # TODO implement context manager API to cleanly handle resource shutdown

    def _classify_item(self, item: ClassificationItem) -> ClassificationResult | None:
        try:
            if not item.topics_skills_embedding_v2:
                self.logger.debug(
                    f"[id={item.id}] no existing topics&skills V2 classification found, running classifier"
                )
                topics_skills_classification = self.topics_skills_v2_classifier.get_classification(item)
            else:
                self.logger.debug(f"[id={item.id}] using existing topics&skills V2 classification")
                topics_skills_classification = TopicsSkillsClassificationV2(
                    topics_skills_embedding_v2=item.topics_skills_embedding_v2,
                    topics_v2=item.topics_v2 or [],  # always set when topics_skills_embedding_v2 exists
                    skills_v2=item.skills_v2 or [],  # always set when topics_skills_embedding_v2 exists
                )
            if not item.similarity_embedding:
                self.logger.debug(f"[id={item.id}] no existing similarity classification found, running classifier")
                similarity_embedding = self.similarity_embedder.embed(item)
            else:
                self.logger.debug(f"[id={item.id}] using existing similarity classification")
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
                f"[id={item.id}] failed to classify. Last model response: {last_response}.",
                exc_info=e,
            )
            return None
        except Exception as e:
            self.logger.error(f"[id={item.id}] failed to classify", exc_info=e)
            return None

    def get_cached_classification(self, item: ClassificationItem) -> ClassificationResult | None:
        query = f"""
            SELECT * FROM `{self.table_name_classifications}`
            WHERE id="{item.id}"
            AND `hash`="{item.hash}"
        """  # noqa: S608
        query_job = self.bqclient.query(query)
        rows = list(query_job.result(max_results=1))
        return rows[0] if rows else None

    def classify_item_and_cache_classification(
        self,
        item: ClassificationItem,
    ) -> ClassificationResult | None:
        self.logger.info(f"[id={item.id}] classifying item")
        try:
            classification = self._classify_item(item)
            if classification is None:
                self.logger.debug(f"[id={item.id}] failed to classify item, skipping")
                return None
            errors = self.bqclient.insert_rows_json(self.table_name_classifications, [classification.to_dict()])
            self.logger.debug(f"[id={item.id}] written to BigQuery")
            if len(errors) > 0:
                raise Exception(f"[id={item.id}] failed to insert classification {classification}: {errors}")
            return classification
        except Exception as e:
            self.logger.error(
                f"[id={item.id}] error while classifying item, skipping, error: %s",
                str(e),
                exc_info=e,
            )
            return None
