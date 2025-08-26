import dataclasses
from typing import Optional
from similarity_embedding import SimilarityEmbedding
from topics_skills_embedding import TopicEmbeddingJson, SkillEmbeddingJson

CONFIDENCE_THRESHOLD = 0.8


def topic_serialization_alias(field_name: str) -> str:
    alias = TopicEmbeddingJson.model_fields[field_name].serialization_alias
    if alias is None:
        raise Exception(f"Missing serialization_alias for field {field_name}")
    return alias


def skill_serialization_alias(field_name: str) -> str:
    alias = SkillEmbeddingJson.model_fields[field_name].serialization_alias
    if alias is None:
        raise Exception(f"Missing serialization_alias for field {field_name}")
    return alias


@dataclasses.dataclass
class Enrichments:
    similarity_embedding: Optional[SimilarityEmbedding]
    topics_v2_embedding: Optional[TopicEmbeddingJson]
    skills_v2_embedding: Optional[SkillEmbeddingJson]

    def topics_v2(self) -> Optional[list[str]]:
        if self.topics_v2_embedding is None:
            return None
        else:
            embedding = self.topics_v2_embedding
            dump = embedding.model_dump()
            return [
                topic_serialization_alias(field_name)
                for field_name, field_confidence in dump.items()
                if dump[field_name] >= CONFIDENCE_THRESHOLD
            ]

    def skills_v2(self) -> Optional[list[str]]:
        if self.skills_v2_embedding is None:
            return None
        else:
            embedding = self.skills_v2_embedding
            dump = embedding.model_dump()
            return [
                skill_serialization_alias(field_name)
                for field_name, field_confidence in dump.items()
                if dump[field_name] >= CONFIDENCE_THRESHOLD
            ]

    def topics_skills_embedding_v2(self) -> Optional[list[float]]:
        if self.topics_v2_embedding is None and self.skills_v2_embedding is None:
            return None

        topics_count = 24
        topics_vector = (
            [0.0 for _ in range(topics_count)]
            if self.topics_v2_embedding is None
            else [
                self.topics_v2_embedding.animal_welfare,
                self.topics_v2_embedding.arts_culture,
                self.topics_v2_embedding.climate_environment,
                self.topics_v2_embedding.democracy_politics,
                self.topics_v2_embedding.disability_inclusion,
                self.topics_v2_embedding.disaster_relief,
                self.topics_v2_embedding.elderly_care,
                self.topics_v2_embedding.family_kids,
                self.topics_v2_embedding.food_hunger_relief,
                self.topics_v2_embedding.gender_equality,
                self.topics_v2_embedding.housing,
                self.topics_v2_embedding.human_rights,
                self.topics_v2_embedding.lgbtqia_plus,
                self.topics_v2_embedding.lifestyle_consumption,
                self.topics_v2_embedding.literacy_education,
                self.topics_v2_embedding.mental_physical_health,
                self.topics_v2_embedding.migration_refugees,
                self.topics_v2_embedding.mobility_transport,
                self.topics_v2_embedding.other,
                self.topics_v2_embedding.peace_security,
                self.topics_v2_embedding.poverty_homelessness,
                self.topics_v2_embedding.sports_recreation,
                self.topics_v2_embedding.technology_innovation,
                self.topics_v2_embedding.youth_development,
            ]
        )
        skills_count = 29
        skills_vector = (
            [0.0 for _ in range(skills_count)]
            if self.skills_v2_embedding is None
            else [
                self.skills_v2_embedding.administration,
                self.skills_v2_embedding.animal_care,
                self.skills_v2_embedding.arts_crafts,
                self.skills_v2_embedding.building_handicraft,
                self.skills_v2_embedding.childcare,
                self.skills_v2_embedding.community_organizing,
                self.skills_v2_embedding.creativity_design,
                self.skills_v2_embedding.driving,
                self.skills_v2_embedding.emergency_care,
                self.skills_v2_embedding.event_planning,
                self.skills_v2_embedding.finances_taxes,
                self.skills_v2_embedding.food_service,
                self.skills_v2_embedding.fundraising,
                self.skills_v2_embedding.gardening_conservation,
                self.skills_v2_embedding.intercultural_competence,
                self.skills_v2_embedding.it_technology,
                self.skills_v2_embedding.legal_assistance,
                self.skills_v2_embedding.management_strategy,
                self.skills_v2_embedding.marketing_communications,
                self.skills_v2_embedding.moderation_facilitation,
                self.skills_v2_embedding.music_performance,
                self.skills_v2_embedding.nursing_caregiving,
                self.skills_v2_embedding.other,
                self.skills_v2_embedding.religious_spiritual_care,
                self.skills_v2_embedding.research_analysis,
                self.skills_v2_embedding.sales_distribution,
                self.skills_v2_embedding.team_leadership,
                self.skills_v2_embedding.translation,
                self.skills_v2_embedding.tutoring_coaching,
            ]
        )
        return topics_vector + skills_vector
