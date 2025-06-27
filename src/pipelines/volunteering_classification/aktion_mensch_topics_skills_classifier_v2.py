from typing import Optional

from shared.classifiers.topics_skills_classifier_v2 import TopicsSkillsClassificationV2
from src.common.logging.PipelineLogger import PipelineLogger
from src.pipelines.volunteering_classification.voltastics_classification_input import VoltasticsClassificationInput
from src.pipelines.volunteering_classification.voltastics_topics_skills_classifier_v2 import (
    VoltasticsTopicsSkillsClassifierV2,
)

logger = PipelineLogger("aktion_mensch_topics_skills_classifier")


class AktionMenschTopicsSkillsClassifierV2(VoltasticsTopicsSkillsClassifierV2):
    """Topics & skills classifier for Aktion Mensch data."""

    def __init__(self, logger: Optional[PipelineLogger] = None) -> None:
        logger_to_use = logger or globals()["logger"]
        super().__init__(logger_to_use)

    def get_classification(self, item: VoltasticsClassificationInput) -> TopicsSkillsClassificationV2:
        """Get classification using volunteering classifier."""
        return super().get_classification(item)
