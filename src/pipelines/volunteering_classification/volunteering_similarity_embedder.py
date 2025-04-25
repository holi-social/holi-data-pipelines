from logging import Logger

from shared.classifiers.classification_item import ClassificationItem
from shared.embedders.similarity_embedder import SimilarityEmbedder


class VolunteeringSimilarityEmbedder(SimilarityEmbedder):
    def __init__(self, logger: Logger):
        super().__init__(logger)

    def _combine_text_fields(self, item: ClassificationItem) -> str:
        combined_text = f"{item.title} {item.description} {' '.join(item.hashTags)} {' '.join(item.uses)}"
        return SimilarityEmbedder._normalize_text(combined_text)
