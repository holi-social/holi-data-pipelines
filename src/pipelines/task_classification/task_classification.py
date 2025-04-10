import logging
from typing import Optional

from prefect.states import Completed, State

from src.common.classifiers.classifier import DEFAULT_BATCH_SIZE, Classifier
from src.common.loaders.bq_loader import wait_for_bigquery_data_to_be_accessible
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.pipelines.bigquery import table_classifications, tasks_without_classifications
from src.pipelines.task_classification.task_similarity_embedder import TaskSimilarityEmbedder
from src.pipelines.task_classification.task_topics_skills_classifier_v2 import TaskTopicsSkillsClassifierV2

logger = PipelineLogger("task_classification")


class TaskClassifier(Classifier):
    def __init__(self, batch_size: Optional[int] = None, stop_after_batch_nr: Optional[int] = None) -> None:
        super().__init__(
            topics_skills_classifier_v2=TaskTopicsSkillsClassifierV2(),
            similarity_embedder=TaskSimilarityEmbedder(),
            item_type="task",
            view_name_without_classifications=tasks_without_classifications(),
            logger=logger,
            batch_size=batch_size or DEFAULT_BATCH_SIZE,
            stop_after_batch_nr=stop_after_batch_nr,
        )


@monitored_flow
def task_classification_flow(
    wait_for_bq: bool = True, batch_size: Optional[int] = None, stop_after_batch_nr: Optional[int] = None
) -> State:
    # TODO use context manager to create and shut down classifier
    classifier = TaskClassifier(batch_size=batch_size, stop_after_batch_nr=stop_after_batch_nr)
    classifier.compute_classifications()
    if wait_for_bq:
        wait_for_bigquery_data_to_be_accessible(table_classifications())
    return Completed()


if __name__ == "__main__":
    logging.getLogger("task_classification").setLevel(logging.DEBUG)
    logging.getLogger("task_topics_skills_classifier_v2").setLevel(logging.DEBUG)
    logging.getLogger("similarity_classification").setLevel(logging.DEBUG)
    task_classification_flow(wait_for_bq=False, batch_size=2, stop_after_batch_nr=1)  # type: ignore
