from typing import Optional
from logging import Logger

from shared.embedders.similarity_embedder import SimilarityEmbedder
from src.common.logging.PipelineLogger import PipelineLogger
from src.pipelines.volunteering_classification.aktion_mensch_classification_input import (
    AktionMenschClassificationInput,
)

logger = PipelineLogger("aktion_mensch_similarity_embedder")


class AktionMenschSimilarityEmbedder(SimilarityEmbedder[AktionMenschClassificationInput]):
    """Similarity embedder for Aktion Mensch data."""

    def __init__(self, logger: Optional[Logger] = None) -> None:
        # Use the provided logger or the default module logger
        logger_to_use = logger or globals()["logger"]
        super().__init__(logger_to_use)

    def _combine_text_fields(self, item: AktionMenschClassificationInput) -> str:
        """Combine text fields for embedding."""
        parts = []
        if item.title:
            parts.append(item.title)
        parts.append(item.description)
        if item.uses and len(item.uses) > 0:
            parts.append(" ".join(item.uses))

        return SimilarityEmbedder._normalize_text(" ".join(parts))
