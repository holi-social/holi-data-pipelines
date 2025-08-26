import dataclasses
from typing import Optional
from pubsub_typesense_updater.similarity_embedding import SimilarityEmbedding
from pubsub_typesense_updater.topics_skills_embedding import TopicEmbeddingJson, SkillEmbeddingJson


@dataclasses.dataclass
class Enrichments:
    similarity_embedding: Optional[SimilarityEmbedding]
    topics_v2_embedding: Optional[TopicEmbeddingJson]
    skills_v2_embedding: Optional[SkillEmbeddingJson]
