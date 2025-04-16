from typing import Protocol, List, Optional, Any


class ClassificationItem(Protocol):
    id: str
    hash: str
    topics_skills_embedding_v2: Optional[List[float]]
    topics_v2: Optional[List[str]]
    skills_v2: Optional[List[str]]
    similarity_embedding: Optional[List[float]]

    # makes mypy type checking happy (means that the data may have arbitrary other fields not listed above)
    def __getattr__(self, name: str) -> Any: ...
