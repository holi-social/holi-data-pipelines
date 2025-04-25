import re
from abc import ABC, abstractmethod
from typing import List, Generic
from logging import Logger

from bs4 import BeautifulSoup
from ratelimit import limits, sleep_and_retry
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel

from shared.classifiers.classification_input import ClassificationInputT

TEXT_EMBEDDING_MODEL_NAME = "text-multilingual-embedding-002"
MODEL_RATE_LIMIT_PER_MINUTE = 2000
EMBEDDING_DIMENSIONALITY = 256
TEXT_EMBEDDING_TASK_TYPE = "RETRIEVAL_DOCUMENT"

type SimilarityEmbedding = List[float]


class SimilarityEmbedder(Generic[ClassificationInputT], ABC):
    model: TextEmbeddingModel
    logger: Logger

    def __init__(self, logger: Logger) -> None:
        self.model = TextEmbeddingModel.from_pretrained(TEXT_EMBEDDING_MODEL_NAME)
        self.logger = logger

    @staticmethod
    def _strip_html_tags(text: str) -> str:
        return BeautifulSoup(text, "html.parser").get_text()

    @staticmethod
    def _normalize_text(text: str) -> str:
        """
        Normalizes text by stripping HTML tags and unnecessary whitespace.
        """
        text = SimilarityEmbedder._strip_html_tags(text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @abstractmethod
    def _combine_text_fields(self, item: ClassificationInputT) -> str:
        pass

    @sleep_and_retry  # type: ignore
    @limits(calls=MODEL_RATE_LIMIT_PER_MINUTE, period=60)  # type: ignore
    def embed(self, item: ClassificationInputT) -> SimilarityEmbedding:
        text_input = TextEmbeddingInput(self._combine_text_fields(item), TEXT_EMBEDDING_TASK_TYPE)
        self.logger.debug(f'Calculating similarity embedding for input: "{text_input}"')
        try:
            embeddings = self.model.get_embeddings([text_input], output_dimensionality=EMBEDDING_DIMENSIONALITY)
            return embeddings[0].values
        except Exception as e:
            self.logger.error(f"Error fetching similarity embedding: {str(e)} for text: {text_input}")
            raise e
