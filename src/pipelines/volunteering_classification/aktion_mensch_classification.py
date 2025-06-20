import logging
from typing import Any, cast
import json

from prefect.states import Completed, State
from shared.classifiers.topics_skills_classifier_v2 import TopicsSkillsClassifierV2
from shared.classifiers.commitment_duration_classifier import CommitmentDurationClassifier
from src.common.classifiers.pipeline_batch_classifier import PipelineBatchClassifier
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.pipelines.bigquery import volunteering_aktion_mensch_without_classifications, table_classifications
from src.pipelines.volunteering_classification.aktion_mensch_classification_input import (
    AktionMenschClassificationInput,
)
from src.pipelines.volunteering_classification.aktion_mensch_similarity_embedder import AktionMenschSimilarityEmbedder
from src.pipelines.volunteering_classification.aktion_mensch_topics_skills_classifier_v2 import (
    AktionMenschTopicsSkillsClassifierV2,
)
from src.pipelines.volunteering_classification.commitment_duration_classifier import (
    VolunteeringCommitmentDurationClassifier,
)
from src.pipelines.volunteering_classification.commitment_labels_mapping import extract_commitment_label_from_item
from src.pipelines.volunteering_classification.location_type_mapping import extract_location_type_from_item
from src.pipelines.volunteering_classification.aktion_mensch_utils import (
    extract_values_from_attributes,
    safe_get_attribute,
)

logger = PipelineLogger("aktion_mensch_classification")
topics_skills_classifier_logger = PipelineLogger("aktion_mensch_topics_skills_classifier")
similarity_embedder_logger = PipelineLogger("aktion_mensch_similarity_embedder")
commitment_duration_classification_logger = PipelineLogger("aktion_mensch_commitment_duration_classification")


class AktionMenschBatchClassifier(PipelineBatchClassifier[AktionMenschClassificationInput]):
    """Classifier for Aktion Mensch volunteering opportunities using PipelineBatchClassifier."""

    def __init__(self, batch_size: int | None = 50, stop_after_batch_nr: int | None = None) -> None:
        # TODO this smells, cast is maybe only required because type parameter is not allowed to be extended
        topics_classifier = cast(
            TopicsSkillsClassifierV2[AktionMenschClassificationInput],
            AktionMenschTopicsSkillsClassifierV2(topics_skills_classifier_logger),
        )
        # TODO this smells, cast is maybe only required because type parameter is not allowed to be extended
        commitment_duration_classifier = cast(
            CommitmentDurationClassifier[AktionMenschClassificationInput],
            VolunteeringCommitmentDurationClassifier(commitment_duration_classification_logger),
        )
        super().__init__(
            topics_skills_classifier_v2=topics_classifier,
            similarity_embedder=AktionMenschSimilarityEmbedder(similarity_embedder_logger),
            commitment_label_extractor=extract_commitment_label_from_item,
            location_type_extractor=extract_location_type_from_item,
            commitment_duration_classifier=commitment_duration_classifier,
            item_type="aktion_mensch_volunteering",
            table_name_classifications=table_classifications(),
            view_name_without_classifications=volunteering_aktion_mensch_without_classifications(),
            logger=logger,
            batch_size=batch_size,
            stop_after_batch_nr=stop_after_batch_nr,
        )

    def _to_classification_input(self, item: Any) -> AktionMenschClassificationInput:
        for field in ["id", "beschreibung", "titel"]:
            if safe_get_attribute(item, field) is None:
                raise ValueError(f"Dictionary missing required field {field}")

        id_value: str = str(safe_get_attribute(item, "id"))
        hash_value: str = str(safe_get_attribute(item, "hash", id_value))
        title: str = str(safe_get_attribute(item, "titel", ""))

        kurzbeschreibung: str = str(safe_get_attribute(item, "kurzbeschreibung", ""))
        beschreibung: str = str(safe_get_attribute(item, "beschreibung", ""))
        description: str = " ".join(filter(None, [kurzbeschreibung, beschreibung]))

        verortung: str = str(safe_get_attribute(item, "verortung", ""))

        attribute: Any = safe_get_attribute(item, "attribute", [])
        if isinstance(attribute, str):
            try:
                attribute = json.loads(attribute)
            except Exception:
                self.logger.warning(f"Failed to parse attribute JSON for item {id_value}")
                attribute = []

        uses = sum(
            [
                extract_values_from_attributes(attribute, attr_type)
                for attr_type in ["kategorie", "taetigkeit", "schlagwort", "zielgruppe"]
            ],
            [],
        )

        return AktionMenschClassificationInput(
            id=id_value,
            hash=hash_value,
            title=title,
            kurzbeschreibung=kurzbeschreibung,
            beschreibung=beschreibung,
            description=description,
            attribute=attribute,
            verortung=verortung,
            uses=uses,
        )


@monitored_flow
def aktion_mensch_classification(batch_size: int | None = 50, stop_after_batch_nr: int | None = None) -> State:
    """Run Aktion Mensch classification pipeline."""
    logger.info("Starting Aktion Mensch classification")
    classifier = AktionMenschBatchClassifier(batch_size=batch_size, stop_after_batch_nr=stop_after_batch_nr)
    classifier.compute_classifications()
    return Completed(message="Aktion Mensch classification completed successfully")


if __name__ == "__main__":
    # Set all loggers to DEBUG level for maximum visibility
    logging.getLogger("shared").setLevel(logging.DEBUG)
    logging.getLogger("src").setLevel(logging.DEBUG)

    # Set specific loggers for this pipeline
    logging.getLogger(logger.name).setLevel(logging.DEBUG)
    logging.getLogger(topics_skills_classifier_logger.name).setLevel(logging.DEBUG)
    logging.getLogger(similarity_embedder_logger.name).setLevel(logging.DEBUG)
    logging.getLogger(commitment_duration_classification_logger.name).setLevel(logging.DEBUG)

    aktion_mensch_classification(batch_size=10, stop_after_batch_nr=0)  # type: ignore
