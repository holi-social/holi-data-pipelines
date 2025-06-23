import logging
from typing import Any
from prefect.states import Completed, State

from shared.classifiers.task_classification_input import TaskClassificationInput
from src.common.classifiers.pipeline_batch_classifier import PipelineBatchClassifier
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.pipelines.bigquery import tasks_without_classifications, table_classifications
from shared.embedders.task_similarity_embedder import TaskSimilarityEmbedder
from shared.classifiers.task_topics_skills_classifier_v2 import TaskTopicsSkillsClassifierV2

logger = PipelineLogger("tasks_classification")
topics_skills_classifier_logger = PipelineLogger("task_topics_skills_classifier_v2")
similarity_embedder_logger = PipelineLogger("task_similarity_embedder")


class TaskBatchClassifier(PipelineBatchClassifier[TaskClassificationInput]):
    def __init__(self, batch_size: int | None = None, stop_after_batch_nr: int | None = None) -> None:
        super().__init__(
            topics_skills_classifier_v2=TaskTopicsSkillsClassifierV2(topics_skills_classifier_logger),
            similarity_embedder=TaskSimilarityEmbedder(similarity_embedder_logger),
            commitment_label_extractor=None,
            location_type_extractor=None,
            commitment_duration_classifier=None,
            item_type="task",
            table_name_classifications=table_classifications(),
            view_name_without_classifications=tasks_without_classifications(),
            logger=logger,
            batch_size=batch_size,
            stop_after_batch_nr=stop_after_batch_nr,
        )

    def _to_classification_input(self, item: Any) -> TaskClassificationInput:
        for field in ["id", "hash", "name", "description"]:
            if not hasattr(item, field):
                logger.error(f"Dictionary missing required field {field}: {item}")
                raise ValueError(f"Dictionary missing required field {field}")
        return TaskClassificationInput(
            id=item.id,
            hash=item.hash,
            name=item.name,
            description=item.description,
            topics_skills_embedding_v2=item.topics_skills_embedding_v2
            if hasattr(item, "topics_skills_embedding_v2")
            else None,
            topics_v2=item.topics_v2 if hasattr(item, "topics_v2") else None,
            skills_v2=item.skills_v2 if hasattr(item, "skills_v2") else None,
            similarity_embedding=item.similarity_embedding,
            location=item.location if hasattr(item, "location") else None,
        )


@monitored_flow
def tasks_classification_flow(batch_size: int | None = None, stop_after_batch_nr: int | None = None) -> State:
    # TODO use context manager to create and shut down classifier
    classifier = TaskBatchClassifier(batch_size=batch_size, stop_after_batch_nr=stop_after_batch_nr)
    classifier.compute_classifications()
    return Completed()


if __name__ == "__main__":
    logging.getLogger(logger.name).setLevel(logging.DEBUG)
    logging.getLogger(topics_skills_classifier_logger.name).setLevel(logging.DEBUG)
    logging.getLogger(similarity_embedder_logger.name).setLevel(logging.DEBUG)
    tasks_classification_flow()  # type: ignore
