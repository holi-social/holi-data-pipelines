from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic
from logging import Logger

from shared.classifiers.classification_input import ClassificationInputT


@dataclass
class CommitmentDurationClassification:
    commitment_duration: str
    commitment_duration_ai_reasoning: str


class CommitmentDurationClassifier(Generic[ClassificationInputT], ABC):
    logger: Logger

    def __init__(self, logger: Logger):
        self.logger = logger

    @abstractmethod
    def get_classification(self, item: ClassificationInputT) -> CommitmentDurationClassification:
        pass
