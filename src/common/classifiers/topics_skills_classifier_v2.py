from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

from src.common.classifiers.classification_item import ClassificationItem
from src.common.logging.PipelineLogger import PipelineLogger


@dataclass
class TopicsSkillsClassificationV2:
    topics_skills_embedding_v2: List[float]
    topics_v2: List[str]
    skills_v2: List[str]


class TopicsSkillsClassifierV2(ABC):
    logger: PipelineLogger

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    @abstractmethod
    def get_classification(self, item: ClassificationItem) -> TopicsSkillsClassificationV2:
        pass
