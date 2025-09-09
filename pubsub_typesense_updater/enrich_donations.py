import dataclasses

from enrichments import Enrichments, normalize_text
from cf_logging import logger
from similarity_embedding import (
    SimilarityEmbedding,
    create_similarity_embedding,
)
from langfuse import get_client
from langchain_core.prompts import PromptTemplate
from topics_skills_embedding import (
    invoke_llm_with_structured_output_cached,
    TopicEmbeddingJson,
    SkillEmbeddingJson,
)
from typesense_document import TypesenseDocument, TypesenseDocumentType
from shared.events import Donation, DonationLink, DonationPicture
from typing import Optional, List


def create_text_input_for_similarity_embedding_from_donation(donation: Donation) -> str:
    text_parts: list[Optional[str]] = [
        donation.title,
        donation.description,
        donation.summary,
        donation.city,
        donation.carrier.name if donation.carrier else None,
        " ".join([category.name or "" for category in donation.categories]),
    ]
    return " ".join([normalize_text(text or "") for text in text_parts])


def create_similarity_embedding_for_donation(donation: Donation) -> SimilarityEmbedding:
    input = create_text_input_for_similarity_embedding_from_donation(donation)
    return create_similarity_embedding(
        input,
        cache_logging_context={
            "logger": logger,
            "log_prefix": f"[id={donation.id} / similarity_embedding]",
        },
    )


def create_topics_v2_embedding_for_donation(donation: Donation) -> TopicEmbeddingJson:
    input = donation.model_dump()
    langfuse = get_client()
    fallback_prompt = """Here is a donation project with title '{{title}}' and decription '{{description}}'.
    Estimate the similarity of the project to the given topics between -1 and 1 (-1 for not similar, 1 for similar)"""
    prompt = langfuse.get_prompt("donations_topics_v2_embedding_prompt", fallback=fallback_prompt)
    # Note: Langfuse uses double curly brackets for variables, while langchain uses single curly brackets
    langchain_prompt = prompt.get_langchain_prompt()
    # only pass in properties used within the prompt template so that the cached LLM invocation only generates a cache
    # miss if property values used in the prompt changes
    input_used_vars = {
        key: normalize_text(str(input[key]))
        for key in PromptTemplate.from_template(langchain_prompt).input_variables
        if key in input
    }
    return TopicEmbeddingJson.model_validate(
        invoke_llm_with_structured_output_cached(
            langchain_prompt=langchain_prompt,
            input=input_used_vars,
            output_json_schema=TopicEmbeddingJson.model_json_schema(),
            cache_logging_context={
                "logger": logger,
                "log_prefix": f"[id={donation.id} / topics_v2_embedding]",
            },
        )
    )


def create_skills_v2_embedding_for_donation(donation: Donation):
    input = donation.model_dump()
    langfuse = get_client()
    fallback_prompt = """Here is a donation project with title '{{title}}' and decription '{{description}}'.
    Estimate the similarity of the project to the given skills between -1 and 1 (-1 for not similar, 1 for similar)"""
    prompt = langfuse.get_prompt("donations_skills_v2_embedding_prompt", fallback=fallback_prompt)
    # Note: Langfuse uses double curly brackets for variables, while langchain uses single curly brackets
    langchain_prompt = prompt.get_langchain_prompt()
    # only pass in properties used within the prompt template so that the cached LLM invocation only generates a cache
    # miss if property values used in the prompt changes
    input_used_vars = {
        key: normalize_text(str(input[key]))
        for key in PromptTemplate.from_template(langchain_prompt).input_variables
        if key in input
    }
    return SkillEmbeddingJson.model_validate(
        invoke_llm_with_structured_output_cached(
            langchain_prompt=langchain_prompt,
            input=input_used_vars,
            output_json_schema=SkillEmbeddingJson.model_json_schema(),
            cache_logging_context={
                "logger": logger,
                "log_prefix": f"[id={donation.id} / skills_v2_embedding]",
            },
        )
    )


@dataclasses.dataclass
class EnrichedDonation(Enrichments):
    donation: Donation


def enrich_donation(donation: Donation) -> EnrichedDonation:
    similarity_embedding = create_similarity_embedding_for_donation(donation)
    topics_embedding = create_topics_v2_embedding_for_donation(donation)
    skills_embedding = create_skills_v2_embedding_for_donation(donation)
    return EnrichedDonation(similarity_embedding, topics_embedding, skills_embedding, donation)


def get_link_with_relation(links: Optional[List[DonationLink]], relation: str) -> Optional[str]:
    if not links or len(links) == 0:
        return None
    links_for_ref = [link for link in links if link.rel == relation]
    if len(links_for_ref) > 0:
        return links_for_ref[0].href
    return None


def get_image_url_from_links(picture: Optional[DonationPicture]) -> Optional[str]:
    if not picture:
        return None
    picture_links = picture.links
    if not picture_links or len(picture_links) == 0:
        return None
    original = get_link_with_relation(picture_links, "original")
    return original or picture_links[0].href


def enriched_donation_to_typesense_doc(enriched: EnrichedDonation) -> TypesenseDocument:
    d = enriched.donation
    return TypesenseDocument(
        id=f"donation_{d.id}",
        type=TypesenseDocumentType("donation"),
        title_de=d.title,
        title_en=d.title_en,
        description_de=d.description,
        description_en=d.description_en,
        location=d.city,
        location_country_de=d.country,
        location_country_en=d.country_en,
        location_type=None,
        location_lat_lng=[d.latitude, d.longitude] if d.latitude and d.longitude else None,
        image_url=get_image_url_from_links(d.profile_picture),
        link_locators={
            "donation": d.id,
        },
        user_full_name=None,
        topics_v2=enriched.topics_v2(),
        skills_v2=enriched.skills_v2(),
        topics_skills_embedding_v2=enriched.topics_skills_embedding_v2(),
        similarity_embedding=enriched.similarity_embedding,
        organizer_name=d.carrier.name if d.carrier else None,
        organizer_image_url=get_image_url_from_links(d.carrier.picture) if d.carrier else None,
        organizer_url=d.carrier_url,
        organizer_location=d.carrier.city if d.carrier else None,
        organizer_location_country_de=d.carrier.country if d.carrier else None,
        organizer_location_country_en=d.carrier_country_en,
        external_url=get_link_with_relation(d.links, "new_donation"),
        source="Betterplace",
        commitment_label=None,
        commitment_duration=None,
        commitment_duration_ai_derived=None,
        commitment_duration_ai_reasoning=None,
        space_id=None,
        contact=None,
        donation_progress_percentage=d.progress_percentage,
        donation_open_amount_in_cents=d.open_amount_in_cents,
        summary_de=d.summary,
        summary_en=d.summary_en,
    )
