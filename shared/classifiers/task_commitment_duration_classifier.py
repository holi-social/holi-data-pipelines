from typing import cast
from shared.classifiers.commitment_duration_classifier import (
    CommitmentDuration,
    CommitmentDurationClassifier,
    CommitmentDurationClassification,
)
from shared.classifiers.task_classification_input import TaskClassificationInput


class TaskCommitmentDurationClassifier(CommitmentDurationClassifier[TaskClassificationInput]):
    def get_classification(self, item: TaskClassificationInput) -> CommitmentDurationClassification:
        return CommitmentDurationClassification(
            commitment_duration=cast(CommitmentDuration, item.duration),
            commitment_duration_ai_derived=False,
            commitment_duration_ai_reasoning=None,
        )
