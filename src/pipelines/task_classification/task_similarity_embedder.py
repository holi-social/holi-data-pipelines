from typing import List

from src.common.classifiers.classification_item import ClassificationItem
from src.common.embedders.similarity_embedder import SimilarityEmbedder


class TaskSimilarityEmbedder(SimilarityEmbedder):
    def _combine_text_fields(self, item: ClassificationItem) -> str:
        topics_v2: List[str] = item.topics_v2 if item.topics_v2 else []
        skills_v2: List[str] = item.skills_v2 if item.skills_v2 else []
        # ATTENTION - if the fields included here change one should also update the `hash` field in the view definition
        # to reflect the fields - this way a re-creation of all embeddings should be triggered
        combined_text = f"{item.name} {item.description} {item.location} {' '.join(topics_v2)} {' '.join(skills_v2)}"
        return SimilarityEmbedder._normalize_text(combined_text)
