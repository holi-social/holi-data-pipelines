from logging import getLogger
from typing import List
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel

from shared.cache import cache
from redis_cache import cache_persistence

type SimilarityEmbedding = List[float]

TEXT_EMBEDDING_MODEL_NAME = "text-multilingual-embedding-002"
EMBEDDING_DIMENSIONALITY = 256
TEXT_EMBEDDING_TASK_TYPE = "RETRIEVAL_DOCUMENT"


logger = getLogger(__name__)


@cache(cache_persistence)
def create_similarity_embedding(
    input: str,
    text_embedding_model_name: str = TEXT_EMBEDDING_MODEL_NAME,
    embedding_dimensionality: int = EMBEDDING_DIMENSIONALITY,
) -> SimilarityEmbedding:
    """
    Invokes a text embedding model to create an embedding for a given text input to be used for similarity comparisons.

    :param input: a single text input - concatenate inputs if there are multiple ones.
    :param text_embedding_model_name: the embedding model to use, defaults to `TEXT_EMBEDDING_MODEL_NAME`
    :param embedding_dimensionality: the dimensionality of the generated embedding, defaults to `EMBEDDING_DIMENSIONALITY`
    :return: embedding vector of dimension `embedding_dimensionality`
    """
    model = TextEmbeddingModel.from_pretrained(text_embedding_model_name)
    text_input = TextEmbeddingInput(text=input, task_type=TEXT_EMBEDDING_TASK_TYPE)
    logger.debug(f'Calculating similarity embedding for input: "{input}"')
    try:
        embeddings = model.get_embeddings([text_input], output_dimensionality=embedding_dimensionality)
        return embeddings[0].values
    except Exception as e:
        logger.error(f"Error fetching similarity embedding: {str(e)} for text: {input}")
        raise e
