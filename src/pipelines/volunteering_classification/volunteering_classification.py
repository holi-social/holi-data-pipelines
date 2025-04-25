from dataclasses import dataclass
import logging
from typing import Optional, List, Any

from prefect.states import Completed, State

from shared.classifiers.classification_input import ClassificationInput
from src.common.classifiers.pipeline_batch_classifier import PipelineBatchClassifier
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.pipelines.bigquery import volunteering_voltastics_without_classifications, table_classifications
from src.pipelines.volunteering_classification.volunteering_topics_skills_classifier_v2 import (
    VolunteeringTopicsSkillsClassifierV2,
)
from src.pipelines.volunteering_classification.volunteering_similarity_embedder import VolunteeringSimilarityEmbedder

logger = PipelineLogger("volunteering_classification")
topics_skills_classification_logger = PipelineLogger("volunteering_topics_skills_classification_v2")
similarity_classification_logger = PipelineLogger("volunteering_similarity_classification")


@dataclass
class VolunteeringClassificationInput(ClassificationInput):
    id: str
    hash: str
    title: str
    description: str
    topics_skills_embedding_v2: Optional[List[float]] = None
    topics_v2: Optional[List[str]] = None
    skills_v2: Optional[List[str]] = None
    similarity_embedding: Optional[List[float]] = None
    hashTags: Optional[List[str]] = None  # noqa: N815
    uses: Optional[List[str]] = None


class VolunteeringBatchClassifier(PipelineBatchClassifier[VolunteeringClassificationInput]):
    def __init__(self, stop_after_batch_nr: Optional[int]) -> None:
        super().__init__(
            topics_skills_classifier_v2=VolunteeringTopicsSkillsClassifierV2(topics_skills_classification_logger),
            similarity_embedder=VolunteeringSimilarityEmbedder(similarity_classification_logger),
            item_type="volunteering",
            table_name_classifications=table_classifications(),
            view_name_without_classifications=volunteering_voltastics_without_classifications(),
            logger=logger,
            stop_after_batch_nr=stop_after_batch_nr,
        )

    def _to_classification_input(self, item: Any) -> VolunteeringClassificationInput:
        for field in ["id", "hash", "title"]:
            if field not in item:
                raise ValueError(f"Dictionary missing required field {field}")
        return VolunteeringClassificationInput(**item)


@monitored_flow
def volunteering_classification(stop_after_batch_nr: Optional[int] = None) -> State:
    # TODO use context manager to create and shut down classifier
    classifier = VolunteeringBatchClassifier(stop_after_batch_nr)
    classifier.compute_classifications()
    return Completed()


if __name__ == "__main__":
    logging.getLogger(logger.name).setLevel(logging.DEBUG)
    logging.getLogger(topics_skills_classification_logger.name).setLevel(logging.DEBUG)
    logging.getLogger(similarity_classification_logger.name).setLevel(logging.DEBUG)
    volunteering_classification(stop_after_batch_nr=0)  # type: ignore
