from typing import List
import re
from bs4 import BeautifulSoup
from src.common.logging.PipelineLogger import PipelineLogger
from src.pipelines.volunteering_classification.topics_skills_classification_v2 import Item
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from ratelimit import limits, sleep_and_retry

TEXT_EMBEDDING_MODEL_NAME = "text-multilingual-embedding-002"
MODEL_RATE_LIMIT_PER_MINUTE = 2000
EMBEDDING_DIMENSIONALITY = 256
TEXT_EMBEDDING_TASK_TYPE = "RETRIEVAL_DOCUMENT"

logger = PipelineLogger("similarity_classification")


class SimilarityClassifier:
    model: TextEmbeddingModel

    def __init__(self) -> None:
        self.model = TextEmbeddingModel.from_pretrained(TEXT_EMBEDDING_MODEL_NAME)

    @staticmethod
    def _strip_html_tags(text: str) -> str:
        return BeautifulSoup(text, "html.parser").get_text()

    @staticmethod
    def _normalize_text(text: str) -> str:
        """
        Normalizes text by stripping HTML tags and unnecessary whitespace.
        """
        text = SimilarityClassifier._strip_html_tags(text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _combine_text_fields(item: Item) -> str:
        combined_text = f"{item['title']} {item['description']} {' '.join(item['hashTags'])} {' '.join(item['uses'])}"
        return SimilarityClassifier._normalize_text(combined_text)

    @sleep_and_retry  # type: ignore
    @limits(calls=MODEL_RATE_LIMIT_PER_MINUTE, period=60)  # type: ignore
    def classify(self, item: Item) -> List[float]:
        text_input = TextEmbeddingInput(SimilarityClassifier._combine_text_fields(item), TEXT_EMBEDDING_TASK_TYPE)
        try:
            embeddings = self.model.get_embeddings([text_input], output_dimensionality=EMBEDDING_DIMENSIONALITY)
            return embeddings[0].values
        except Exception as e:
            logger.error(f"Error fetching similarity embedding: {str(e)} for text: {text_input}")
            raise e
