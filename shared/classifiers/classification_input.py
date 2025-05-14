from typing import Protocol, Optional, List, TypeVar


class ClassificationInput(Protocol):
    id: str
    hash: str
    topics_skills_embedding_v2: Optional[List[float]]
    topics_v2: Optional[List[str]]
    skills_v2: Optional[List[str]]
    similarity_embedding: Optional[List[float]]


ClassificationInputT = TypeVar("ClassificationInputT", bound=ClassificationInput)
