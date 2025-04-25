from logging import Logger
from typing import List, Optional

from shared.classifiers.topics_skills_classifier_v2 import TopicsSkillsClassifierV2, TopicsSkillsClassificationV2
from shared.classifiers.task_classification_input import TaskClassificationInput

TOPICS_V2_VECTOR_INDICES = {
    "animal-welfare": 0,
    "arts-culture": 1,
    "climate-environment": 2,
    "democracy-politics": 3,
    "disability-inclusion": 4,
    "disaster-relief": 5,
    "elderly-care": 6,
    "family-kids": 7,
    "food-hunger-relief": 8,
    "gender-equality": 9,
    "housing": 10,
    "human-rights": 11,
    "lgbtqia-plus": 12,
    "lifestyle-consumption": 13,
    "literacy-education": 14,
    "mental-physical-health": 15,
    "migration-refugees": 16,
    "mobility-transport": 17,
    "other": 18,
    "peace-security": 19,
    "poverty-homelessness": 20,
    "sports-recreation": 21,
    "technology-innovation": 22,
    "youth-development": 23,
}

SKILLS_V2_VECTOR_INDICES = {
    "administration": 0,
    "animal-care": 1,
    "arts-crafts": 2,
    "building-handicraft": 3,
    "childcare": 4,
    "community-organizing": 5,
    "creativity-design": 6,
    "driving": 7,
    "emergency-care": 8,
    "event-planning": 9,
    "finances-taxes": 10,
    "food-service": 11,
    "fundraising": 12,
    "gardening-conservation": 13,
    "intercultural-competence": 14,
    "it-technology": 15,
    "legal-assistance": 16,
    "management-strategy": 17,
    "marketing-communications": 18,
    "moderation-facilitation": 19,
    "music-performance": 20,
    "nursing-caregiving": 21,
    "other": 22,
    "religious-spiritual-care": 23,
    "research-analysis": 24,
    "sales-distribution": 25,
    "team-leadership": 26,
    "translation": 27,
    "tutoring-coaching": 28,
}


def to_topics_skills_embedding_v2(topics_v2: Optional[List[str]], skills_v2: Optional[List[str]]) -> List[float]:
    # create a zero-filled vector with length equal to the number of skills
    vector_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
    embedding = [0.0] * vector_size

    # for each topic in the list, set its corresponding position to 1.0
    if topics_v2:
        for topic in topics_v2:
            if topic in TOPICS_V2_VECTOR_INDICES:
                embedding[TOPICS_V2_VECTOR_INDICES[topic]] = 1.0

    # for each skill in the list, set its corresponding position to 1.0
    if skills_v2:
        for skill in skills_v2:
            if skill in SKILLS_V2_VECTOR_INDICES:
                embedding[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES[skill]] = 1.0

    return embedding


class TaskTopicsSkillsClassifierV2(TopicsSkillsClassifierV2[TaskClassificationInput]):
    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)

    def get_classification(self, item: TaskClassificationInput) -> TopicsSkillsClassificationV2:
        topics_v2 = item.topics_v2 or []
        skills_v2 = item.skills_v2 or []
        return TopicsSkillsClassificationV2(
            topics_skills_embedding_v2=to_topics_skills_embedding_v2(topics_v2, skills_v2),
            topics_v2=topics_v2,
            skills_v2=skills_v2,
        )
