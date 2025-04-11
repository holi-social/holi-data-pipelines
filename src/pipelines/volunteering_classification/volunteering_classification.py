import logging
from typing import Optional

from prefect.states import Completed, State

from src.common.classifiers.classifier import Classifier
from src.common.loaders.bq_loader import wait_for_bigquery_data_to_be_accessible
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.pipelines.bigquery import table_classifications, volunteering_voltastics_without_classifications
from src.pipelines.volunteering_classification.volunteering_topics_skills_classifier_v2 import (
    VolunteeringTopicsSkillsClassifierV2,
)
from src.pipelines.volunteering_classification.volunteering_similarity_embedder import VolunteeringSimilarityEmbedder

logger = PipelineLogger("volunteering_classification")


class VolunteeringClassifier(Classifier):
    def __init__(self, stop_after_batch_nr: Optional[int]) -> None:
        super().__init__(
            topics_skills_classifier_v2=VolunteeringTopicsSkillsClassifierV2(),
            similarity_embedder=VolunteeringSimilarityEmbedder(),
            item_type="volunteering",
            view_name_without_classifications=volunteering_voltastics_without_classifications(),
            logger=logger,
            stop_after_batch_nr=stop_after_batch_nr,
        )


@monitored_flow
def volunteering_classification(wait_for_bq: bool = True, stop_after_batch_nr: Optional[int] = None) -> State:
    # TODO use context manager to create and shut down classifier
    classifier = VolunteeringClassifier(stop_after_batch_nr)
    classifier.compute_classifications()
    if wait_for_bq:
        wait_for_bigquery_data_to_be_accessible(table_classifications())
    return Completed()


if __name__ == "__main__":
    logging.getLogger("volunteering_classification").setLevel(logging.DEBUG)
    logging.getLogger("volunteering_topics_skills_classification_v2").setLevel(logging.DEBUG)
    logging.getLogger("similarity_classification").setLevel(logging.DEBUG)
    volunteering_classification(wait_for_bq=False, stop_after_batch_nr=0)  # type: ignore
