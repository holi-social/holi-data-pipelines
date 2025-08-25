from pydantic import BaseModel, Field
from langchain_core.prompts import PromptTemplate
from typing import Dict, Any
from langchain_google_vertexai import ChatVertexAI

from shared.cache import cache
from pubsub_typesense_updater.redis_cache import cache_persistence


MODEL_NAME = "gemini-2.5-flash"


class TopicEmbeddingJson(BaseModel):
    animal_welfare: float = Field(default=None, serialization_alias="animal-welfare", description="Animal Welfare")
    arts_culture: float = Field(default=None, serialization_alias="arts-culture", description="Arts & Culture")
    climate_environment: float = Field(
        default=None, serialization_alias="climate-environment", description="Climate & Environment"
    )
    democracy_politics: float = Field(
        default=None, serialization_alias="democracy-politics", description="Democracy & Politics"
    )
    disability_inclusion: float = Field(
        default=None, serialization_alias="disability-inclusion", description="Disability & Inclusion"
    )
    disaster_relief: float = Field(default=None, serialization_alias="disaster-relief", description="Disaster Relief")
    elderly_care: float = Field(default=None, serialization_alias="elderly-care", description="Elderly Care")
    family_kids: float = Field(default=None, serialization_alias="family-kids", description="Family & Kids")
    food_hunger_relief: float = Field(
        default=None, serialization_alias="food-hunger-relief", description="Food & Hunger Relief"
    )
    gender_equality: float = Field(default=None, serialization_alias="gender-equality", description="Gender Equality")
    housing: float = Field(default=None, serialization_alias="housing", description="Housing")
    human_rights: float = Field(default=None, serialization_alias="human-rights", description="Human Rights")
    lgbtqia_plus: float = Field(default=None, serialization_alias="lgbtqia-plus", description="LGBTQIA+")
    lifestyle_consumption: float = Field(
        default=None, serialization_alias="lifestyle-consumption", description="Lifestyle & Consumption"
    )
    literacy_education: float = Field(
        default=None, serialization_alias="literacy-education", description="Literacy & Education"
    )
    mental_physical_health: float = Field(
        default=None, serialization_alias="mental-physical-health", description="Mental & Physical Health"
    )
    migration_refugees: float = Field(
        default=None, serialization_alias="migration-refugees", description="Migration & Refugees"
    )
    mobility_transport: float = Field(
        default=None, serialization_alias="mobility-transport", description="Mobility & Transport"
    )
    other: float = Field(default=None, serialization_alias="other", description="Other")
    peace_security: float = Field(default=None, serialization_alias="peace-security", description="Peace & Security")
    poverty_homelessness: float = Field(
        default=None, serialization_alias="poverty-homelessness", description="Poverty & Homelessness"
    )
    sports_recreation: float = Field(
        default=None, serialization_alias="sports-recreation", description="Sports & Recreation"
    )
    technology_innovation: float = Field(
        default=None, serialization_alias="technology-innovation", description="Technology & Innovation"
    )
    youth_development: float = Field(
        default=None, serialization_alias="youth-development", description="Youth Development"
    )

    class Config:
        populate_by_name = True  # Allows using both field names and aliases for parsing


class SkillEmbeddingJson(BaseModel):
    administration: float = Field(default=None, serialization_alias="administration", description="Administration")
    animal_care: float = Field(default=None, serialization_alias="animal-care", description="Animal Care")
    arts_crafts: float = Field(default=None, serialization_alias="arts-crafts", description="Arts & Crafts")
    building_handicraft: float = Field(
        default=None, serialization_alias="building-handicraft", description="Building & Handicraft"
    )
    childcare: float = Field(default=None, serialization_alias="childcare", description="Childcare")
    community_organizing: float = Field(
        default=None, serialization_alias="community-organizing", description="Community Organizing"
    )
    creativity_design: float = Field(
        default=None, serialization_alias="creativity-design", description="Creativity & Design"
    )
    driving: float = Field(default=None, serialization_alias="driving", description="Driving")
    emergency_care: float = Field(default=None, serialization_alias="emergency-care", description="Emergency Care")
    event_planning: float = Field(default=None, serialization_alias="event-planning", description="Event Planning")
    finances_taxes: float = Field(default=None, serialization_alias="finances-taxes", description="Finances & Taxes")
    food_service: float = Field(default=None, serialization_alias="food-service", description="Food Service")
    fundraising: float = Field(default=None, serialization_alias="fundraising", description="Fundraising")
    gardening_conservation: float = Field(
        default=None, serialization_alias="gardening-conservation", description="Gardening & Conservation"
    )
    intercultural_competence: float = Field(
        default=None, serialization_alias="intercultural-competence", description="Intercultural Competence"
    )
    it_technology: float = Field(default=None, serialization_alias="it-technology", description="IT & Technology")
    legal_assistance: float = Field(
        default=None, serialization_alias="legal-assistance", description="Legal Assistance"
    )
    management_strategy: float = Field(
        default=None, serialization_alias="management-strategy", description="Management & Strategy"
    )
    marketing_communications: float = Field(
        default=None, serialization_alias="marketing-communications", description="Marketing & Communications"
    )
    moderation_facilitation: float = Field(
        default=None, serialization_alias="moderation-facilitation", description="Moderation & Facilitation"
    )
    music_performance: float = Field(
        default=None, serialization_alias="music-performance", description="Music & Performance"
    )
    nursing_caregiving: float = Field(
        default=None, serialization_alias="nursing-caregiving", description="Nursing & Caregiving"
    )
    other: float = Field(default=None, serialization_alias="other", description="Other")
    religious_spiritual_care: float = Field(
        default=None, serialization_alias="religious-spiritual-care", description="Religious & Spiritual Care"
    )
    research_analysis: float = Field(
        default=None, serialization_alias="research-analysis", description="Research & Analysis"
    )
    sales_distribution: float = Field(
        default=None, serialization_alias="sales-distribution", description="Sales & Distribution"
    )
    team_leadership: float = Field(default=None, serialization_alias="team-leadership", description="Team Leadership")
    translation: float = Field(default=None, serialization_alias="translation", description="Translation")
    tutoring_coaching: float = Field(
        default=None, serialization_alias="tutoring-coaching", description="Tutoring & Coaching"
    )

    class Config:
        populate_by_name = True  # Allows using both field names and aliases for parsing


@cache(cache_persistence)
def create_topics_skills_v2_embedding(
    prompt_template: PromptTemplate,
    input: Dict[Any, Any],
    model_name: str = MODEL_NAME,
):
    llm = ChatVertexAI(
        model_name=MODEL_NAME,
        project="holi-shared",
        location="europe-north1",
        temperature=0.7,
        max_retries=0,
        timeout=30,
    )
    model_with_structure = llm.with_structured_output(TopicEmbeddingJson)
    response = prompt_template.invoke(input)
    structured_output = model_with_structure.invoke(response)

    return structured_output
