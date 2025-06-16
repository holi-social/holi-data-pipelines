from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Literal
from logging import Logger

from shared.classifiers.classification_input import ClassificationInputT


CommitmentDuration = Literal[
    "MINUTES_10to15",
    "MINUTES_15to30",
    "MINUTES_30to60",
    "HOURS_1to2",
    "HOURS_2to4",
    "HOURS_4to6",
    "DAYS_1",
    "DAYS_2",
    "DAYS_3_OR_MORE",
    "INDETERMINATE",
]


@dataclass
class CommitmentDurationClassification:
    commitment_duration: CommitmentDuration
    commitment_duration_ai_reasoning: str


class CommitmentDurationClassifier(Generic[ClassificationInputT], ABC):
    logger: Logger

    def __init__(self, logger: Logger):
        self.logger = logger

    @abstractmethod
    def get_classification(self, item: ClassificationInputT) -> CommitmentDurationClassification:
        pass
