from logging import getLogger
import re
from typing import List
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from bs4 import BeautifulSoup

from shared.cache import cache
from pubsub_typesense_updater.redis_cache import cache_persistence

type SimilarityEmbedding = List[float]

TEXT_EMBEDDING_MODEL_NAME = "text-multilingual-embedding-002"
EMBEDDING_DIMENSIONALITY = 256
TEXT_EMBEDDING_TASK_TYPE = "RETRIEVAL_DOCUMENT"


logger = getLogger(__name__)


# TODO(SF) write docs
@cache(cache_persistence)
def create_similarity_embedding(
    input: str,
    text_embedding_model_name: str = TEXT_EMBEDDING_MODEL_NAME,
    embedding_dimensionality: int = EMBEDDING_DIMENSIONALITY,
) -> SimilarityEmbedding:
    print("cache miss similarity embedding")
    model = TextEmbeddingModel.from_pretrained(text_embedding_model_name)
    text_input = TextEmbeddingInput(text=input, task_type=TEXT_EMBEDDING_TASK_TYPE)
    logger.debug(f'Calculating similarity embedding for input: "{input}"')
    try:
        embeddings = model.get_embeddings([text_input], output_dimensionality=embedding_dimensionality)
        return embeddings[0].values
    except Exception as e:
        logger.error(f"Error fetching similarity embedding: {str(e)} for text: {input}")
        raise e


def normalize_text(input: str) -> str:
    """
    Normalizes text by stripping HTML tags and unnecessary whitespace.
    """
    text = BeautifulSoup(input, "html.parser").get_text()
    return re.sub(r"\s+", " ", text).strip()
