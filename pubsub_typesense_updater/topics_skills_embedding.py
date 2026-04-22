import os

from pydantic import BaseModel, Field
from typing import Dict, Any
from langchain_google_vertexai import ChatVertexAI

from shared.cache import cache
from redis_cache import cache_persistence


MODEL_NAME = "gemini-2.5-flash"


# TODO verify / guarantee values to be between -1 and 1 (?)
class TopicEmbeddingJson(BaseModel):
    animal_welfare: float = Field(serialization_alias="animal-welfare", description="Animal Welfare")
    arts_culture: float = Field(serialization_alias="arts-culture", description="Arts & Culture")
    climate_environment: float = Field(serialization_alias="climate-environment", description="Climate & Environment")
    democracy_politics: float = Field(serialization_alias="democracy-politics", description="Democracy & Politics")
    disability_inclusion: float = Field(
        serialization_alias="disability-inclusion", description="Disability & Inclusion"
    )
    disaster_relief: float = Field(serialization_alias="disaster-relief", description="Disaster Relief")
    elderly_care: float = Field(serialization_alias="elderly-care", description="Elderly Care")
    family_kids: float = Field(serialization_alias="family-kids", description="Family & Kids")
    food_hunger_relief: float = Field(serialization_alias="food-hunger-relief", description="Food & Hunger Relief")
    gender_equality: float = Field(serialization_alias="gender-equality", description="Gender Equality")
    housing: float = Field(serialization_alias="housing", description="Housing")
    human_rights: float = Field(serialization_alias="human-rights", description="Human Rights")
    lgbtqia_plus: float = Field(serialization_alias="lgbtqia-plus", description="LGBTQIA+")
    lifestyle_consumption: float = Field(
        serialization_alias="lifestyle-consumption", description="Lifestyle & Consumption"
    )
    literacy_education: float = Field(serialization_alias="literacy-education", description="Literacy & Education")
    mental_physical_health: float = Field(
        serialization_alias="mental-physical-health", description="Mental & Physical Health"
    )
    migration_refugees: float = Field(serialization_alias="migration-refugees", description="Migration & Refugees")
    mobility_transport: float = Field(serialization_alias="mobility-transport", description="Mobility & Transport")
    other: float = Field(serialization_alias="other", description="Other")
    peace_security: float = Field(serialization_alias="peace-security", description="Peace & Security")
    poverty_homelessness: float = Field(
        serialization_alias="poverty-homelessness", description="Poverty & Homelessness"
    )
    sports_recreation: float = Field(serialization_alias="sports-recreation", description="Sports & Recreation")
    technology_innovation: float = Field(
        serialization_alias="technology-innovation", description="Technology & Innovation"
    )
    youth_development: float = Field(serialization_alias="youth-development", description="Youth Development")

    class Config:
        populate_by_name = True  # Allows using both field names and aliases for parsing


# TODO verify / guarantee values to be between -1 and 1 (?)
class SkillEmbeddingJson(BaseModel):
    administration: float = Field(serialization_alias="administration", description="Administration")
    animal_care: float = Field(serialization_alias="animal-care", description="Animal Care")
    arts_crafts: float = Field(serialization_alias="arts-crafts", description="Arts & Crafts")
    building_handicraft: float = Field(serialization_alias="building-handicraft", description="Building & Handicraft")
    childcare: float = Field(serialization_alias="childcare", description="Childcare")
    community_organizing: float = Field(serialization_alias="community-organizing", description="Community Organizing")
    creativity_design: float = Field(serialization_alias="creativity-design", description="Creativity & Design")
    driving: float = Field(serialization_alias="driving", description="Driving")
    emergency_care: float = Field(serialization_alias="emergency-care", description="Emergency Care")
    event_planning: float = Field(serialization_alias="event-planning", description="Event Planning")
    finances_taxes: float = Field(serialization_alias="finances-taxes", description="Finances & Taxes")
    food_service: float = Field(serialization_alias="food-service", description="Food Service")
    fundraising: float = Field(serialization_alias="fundraising", description="Fundraising")
    gardening_conservation: float = Field(
        serialization_alias="gardening-conservation", description="Gardening & Conservation"
    )
    intercultural_competence: float = Field(
        serialization_alias="intercultural-competence", description="Intercultural Competence"
    )
    it_technology: float = Field(serialization_alias="it-technology", description="IT & Technology")
    legal_assistance: float = Field(serialization_alias="legal-assistance", description="Legal Assistance")
    management_strategy: float = Field(serialization_alias="management-strategy", description="Management & Strategy")
    marketing_communications: float = Field(
        serialization_alias="marketing-communications", description="Marketing & Communications"
    )
    moderation_facilitation: float = Field(
        serialization_alias="moderation-facilitation", description="Moderation & Facilitation"
    )
    music_performance: float = Field(serialization_alias="music-performance", description="Music & Performance")
    nursing_caregiving: float = Field(serialization_alias="nursing-caregiving", description="Nursing & Caregiving")
    other: float = Field(serialization_alias="other", description="Other")
    religious_spiritual_care: float = Field(
        serialization_alias="religious-spiritual-care", description="Religious & Spiritual Care"
    )
    research_analysis: float = Field(serialization_alias="research-analysis", description="Research & Analysis")
    sales_distribution: float = Field(serialization_alias="sales-distribution", description="Sales & Distribution")
    team_leadership: float = Field(serialization_alias="team-leadership", description="Team Leadership")
    translation: float = Field(serialization_alias="translation", description="Translation")
    tutoring_coaching: float = Field(serialization_alias="tutoring-coaching", description="Tutoring & Coaching")

    class Config:
        populate_by_name = True  # Allows using both field names and aliases for parsing


@cache(cache_persistence)
def invoke_llm_with_structured_output_cached(
    prompt: str,
    output_json_schema: Dict[str, Any],
    model_name: str = MODEL_NAME,
) -> dict[str, Any]:
    """
    Invokes an LLM and returns structured output. Caches the result based on parameter values and this methods' body.

    Relies on JSON schema because of 2 reasons:
    1. LLMs then use controlled genetration to generate the response rather than function calling (faster).
    2. Passing the model class is not (easily) serializable, so it can't be used for cache key generation.

    :param prompt: the prompt to execute
    :param output_json_schema:
    :param model_name: the LLM models name
    :return: a dict conforming to the output schema
    """
    llm = ChatVertexAI(
        model_name=model_name,
        project=os.getenv("VERTEX_AI_PROJECT"),
        location=os.getenv("VERTEX_AI_LOCATION"),
        max_retries=int(os.getenv("VERTEX_AI_RETRIES", 2)),
    )
    model_with_structure = llm.with_structured_output(output_json_schema, method="json_mode")
    structured_output = model_with_structure.invoke(prompt)

    return structured_output
