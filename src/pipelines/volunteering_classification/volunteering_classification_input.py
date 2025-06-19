from dataclasses import dataclass
from typing import Optional, List, Any

from shared.classifiers.classification_input import ClassificationInput


@dataclass
class VolunteeringClassificationInput(ClassificationInput):
    id: str
    hash: str
    title: str
    description: str
    topics_skills_embedding_v2: Optional[List[float]] = None
    topics_v2: Optional[List[str]] = None
    skills_v2: Optional[List[str]] = None
    similarity_embedding: Optional[List[float]] = None
    hashTags: Optional[List[str]] = None  # noqa: N815
    uses: Optional[List[str]] = None
    source: Optional[str] = None
    additional_data: Optional[Any] = None
    type: str = "volunteering"
