from dataclasses import dataclass
from typing import Optional, List

from shared.classifiers.classification_input import ClassificationInput


@dataclass
class TaskClassificationInput(ClassificationInput):
    id: str
    hash: str
    name: str
    description: str
    topics_skills_embedding_v2: Optional[List[float]] = None
    topics_v2: Optional[List[str]] = None
    skills_v2: Optional[List[str]] = None
    similarity_embedding: Optional[List[float]] = None
    location: Optional[str] = None
