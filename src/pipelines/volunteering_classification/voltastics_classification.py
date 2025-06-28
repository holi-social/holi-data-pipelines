import logging
from typing import Any

from prefect import flow
from prefect.states import Completed, State

from src.common.classifiers.pipeline_batch_classifier import PipelineBatchClassifier, DEFAULT_BATCH_SIZE
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import (
    on_cancellation_hooks,
    on_crashed_hooks,
    on_failure_hooks,
    on_completion_hooks,
)

from src.pipelines.volunteering_classification.voltastics_classification_input import VoltasticsClassificationInput
from src.pipelines.bigquery import volunteering_voltastics_without_classifications, table_classifications
from src.pipelines.volunteering_classification.voltastics_topics_skills_classifier_v2 import (
    VoltasticsTopicsSkillsClassifierV2,
)
from src.pipelines.volunteering_classification.commitment_duration_classifier import (
    VolunteeringCommitmentDurationClassifier,
)
from src.pipelines.volunteering_classification.voltastics_similarity_embedder import VoltasticsSimilarityEmbedder

from src.pipelines.volunteering_classification.commitment_labels_mapping import (
    extract_commitment_label_from_item,
)
from src.pipelines.volunteering_classification.location_type_mapping import (
    extract_location_type_from_item,
)

logger = PipelineLogger("volunteering_classification")
topics_skills_classification_logger = PipelineLogger("volunteering_topics_skills_classification_v2")
similarity_classification_logger = PipelineLogger("volunteering_similarity_classification")
commitment_duration_classification_logger = PipelineLogger("volunteering_commitment_duration_classification")


class VoltasticsBatchClassifier(PipelineBatchClassifier[VoltasticsClassificationInput]):
    def __init__(self, batch_size: int | None = DEFAULT_BATCH_SIZE, stop_after_batch_nr: int | None = None) -> None:
        super().__init__(
            topics_skills_classifier_v2=VoltasticsTopicsSkillsClassifierV2(topics_skills_classification_logger),
            similarity_embedder=VoltasticsSimilarityEmbedder(similarity_classification_logger),
            commitment_label_extractor=extract_commitment_label_from_item,
            location_type_extractor=extract_location_type_from_item,
            commitment_duration_classifier=VolunteeringCommitmentDurationClassifier(
                commitment_duration_classification_logger
            ),
            item_type="volunteering",
            table_name_classifications=table_classifications(),
            view_name_without_classifications=volunteering_voltastics_without_classifications(),
            logger=logger,
            batch_size=batch_size,
            stop_after_batch_nr=stop_after_batch_nr,
        )

    def _to_classification_input(self, item: Any) -> VoltasticsClassificationInput:
        for field in ["id", "hash", "title", "description"]:
            if not hasattr(item, field):
                raise ValueError(f"Dictionary missing required field {field}")
        return VoltasticsClassificationInput(
            id=item.id,
            hash=item.hash,
            title=item.title,
            description=item.description,
            topics_skills_embedding_v2=item.topics_skills_embedding_v2
            if hasattr(item, "topics_skills_embedding_v2")
            else None,
            topics_v2=item.topics_v2 if hasattr(item, "topics_v2") else None,
            skills_v2=item.skills_v2 if hasattr(item, "skills_v2") else None,
            similarity_embedding=item.similarity_embedding if hasattr(item, "similarity_embedding") else None,
            hashTags=item.hashTags if hasattr(item, "hashTags") else None,
            uses=item.uses if hasattr(item, "uses") else None,
            source=item.source if hasattr(item, "source") else None,
            additional_data=item.additionalData if hasattr(item, "additionalData") else None,
        )


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def voltastics_classification_flow(
    batch_size: int | None = DEFAULT_BATCH_SIZE, stop_after_batch_nr: int | None = None
) -> State:
    classifier = VoltasticsBatchClassifier(batch_size=batch_size, stop_after_batch_nr=stop_after_batch_nr)
    classifier.compute_classifications()
    return Completed(message="Volunteering classification completed successfully")


if __name__ == "__main__":
    # Set all loggers to DEBUG level for maximum visibility
    logging.getLogger("shared").setLevel(logging.DEBUG)
    logging.getLogger("src").setLevel(logging.DEBUG)

    # Set specific loggers for this pipeline
    logging.getLogger(logger.name).setLevel(logging.DEBUG)
    logging.getLogger(topics_skills_classification_logger.name).setLevel(logging.DEBUG)
    logging.getLogger(similarity_classification_logger.name).setLevel(logging.DEBUG)
    logging.getLogger(commitment_duration_classification_logger.name).setLevel(logging.DEBUG)

    voltastics_classification_flow(batch_size=10, stop_after_batch_nr=0)  # type: ignore
