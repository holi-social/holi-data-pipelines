from logging import getLogger
import os
from typing import List
from langchain_google_vertexai import VertexAIEmbeddings

from shared.cache import cache
from redis_cache import cache_persistence

type SimilarityEmbedding = List[float]

EMBEDDING_DIMENSIONALITY = 256

logger = getLogger(__name__)


# TODO(SF) write docs
@cache(cache_persistence)
def create_similarity_embedding(input: str) -> SimilarityEmbedding:
    embeddings_model = VertexAIEmbeddings(
        model_name="text-multilingual-embedding-002",
        project=os.getenv("VERTEX_AI_PROJECT"),
        location=os.getenv("VERTEX_AI_LOCATION"),
        max_retries=int(os.getenv("VERTEX_AI_RETRIES", 2)),
    )
    logger.debug(f'Calculating similarity embedding for input: "{input}"')
    try:
        embeddings = embeddings_model.embed([input], dimensions=256, embeddings_task_type="RETRIEVAL_DOCUMENT")
        return embeddings[0]
    except Exception as e:
        logger.error(f"Error fetching similarity embedding: {str(e)} for text: {input}")
        raise e
