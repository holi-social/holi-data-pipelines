from typing import Any

from src.common.embedders.similarity_embedder import SimilarityEmbedder


class VolunteeringSimilarityEmbedder(SimilarityEmbedder):
    def _combine_text_fields(self, item: Any) -> str:
        combined_text = f"{item['title']} {item['description']} {' '.join(item['hashTags'])} {' '.join(item['uses'])}"
        return SimilarityEmbedder._normalize_text(combined_text)
