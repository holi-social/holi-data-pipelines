from logging import Logger

from shared.embedders.similarity_embedder import SimilarityEmbedder
from src.pipelines.volunteering_classification.volunteering_classification import VolunteeringClassificationInput


class VolunteeringSimilarityEmbedder(SimilarityEmbedder[VolunteeringClassificationInput]):
    def __init__(self, logger: Logger):
        super().__init__(logger)

    def _combine_text_fields(self, item: VolunteeringClassificationInput) -> str:
        combined_text = f"{item.title} {item.description} {' '.join(item.hashTags or [])} {' '.join(item.uses or [])}"
        return SimilarityEmbedder._normalize_text(combined_text)
