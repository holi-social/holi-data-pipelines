from logging import Logger

from shared.classifiers.task_classification_input import TaskClassificationInput
from shared.embedders.similarity_embedder import SimilarityEmbedder


class TaskSimilarityEmbedder(SimilarityEmbedder[TaskClassificationInput]):
    def __init__(self, logger: Logger):
        super().__init__(logger)

    def _combine_text_fields(self, item: TaskClassificationInput) -> str:
        # ATTENTION - if the fields included here change one should also update the `hash` field in the view definition
        # to reflect the fields - this way a re-creation of all embeddings should be triggered
        combined_text = f"{item.name} {item.description} {item.location or ''} {' '.join(item.topics_v2 or [])} {' '.join(item.skills_v2 or [])}"
        return SimilarityEmbedder._normalize_text(combined_text)
