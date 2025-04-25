from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List
from logging import Logger

from shared.classifiers.classification_item import ClassificationItem


@dataclass
class TopicsSkillsClassificationV2:
    topics_skills_embedding_v2: List[float]
    topics_v2: List[str]
    skills_v2: List[str]


class TopicsSkillsClassifierV2(ABC):
    logger: Logger

    def __init__(self, logger: Logger):
        self.logger = logger

    @abstractmethod
    def get_classification(self, item: ClassificationItem) -> TopicsSkillsClassificationV2:
        pass
