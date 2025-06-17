from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Literal, get_args, cast, Optional
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


def parse_commitment_duration(input: str) -> CommitmentDuration:
    if input in get_args(CommitmentDuration):
        return cast(CommitmentDuration, input)
    raise ValueError(
        f"Unexpect value '{input}' for CommitmentDuration. Valid values are {get_args(CommitmentDuration)}"
    )


@dataclass
class CommitmentDurationClassification:
    commitment_duration: CommitmentDuration
    commitment_duration_ai_reasoning: Optional[str]


class CommitmentDurationClassifier(Generic[ClassificationInputT], ABC):
    logger: Logger

    def __init__(self, logger: Logger):
        self.logger = logger

    @abstractmethod
    def get_classification(self, item: ClassificationInputT) -> CommitmentDurationClassification:
        pass
