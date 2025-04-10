from dataclasses import dataclass, asdict
from typing import List, Dict, Any


@dataclass
class ClassificationResult:
    type: str
    id: str
    hash: str
    topics_skills_embedding_v2: List[float]
    topics_v2: List[str]
    skills_v2: List[str]
    similarity_embedding: List[float]

    def to_dict(self) -> Dict[str, Any]:
        # noinspection PyTypeChecker
        return asdict(self)
