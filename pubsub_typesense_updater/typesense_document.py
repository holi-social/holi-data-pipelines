from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from enum import Enum


class LocationType(str, Enum):
    onsite = "onsite"
    remote = "remote"


class CommitmentLabel(str, Enum):
    one_time = "one-time"
    recurring = "recurring"
    negotiable = "negotiable"


class CommitmentDuration(str, Enum):
    DAYS_1 = "DAYS_1"
    DAYS_2 = "DAYS_2"
    DAYS_3_OR_MORE = "DAYS_3_OR_MORE"
    HOURS_1to2 = "HOURS_1to2"
    HOURS_2to4 = "HOURS_2to4"
    HOURS_4to6 = "HOURS_4to6"
    INDETERMINATE = "INDETERMINATE"
    MINUTES_10to15 = "MINUTES_10to15"
    MINUTES_15to30 = "MINUTES_15to30"
    MINUTES_30to60 = "MINUTES_30to60"


class ContactType(str, Enum):
    person = "person"
    email = "email"
    phone = "phone"
    website = "website"


class ContactInfo(BaseModel):
    type: ContactType
    value: str
    label: Optional[str]


class TypesenseDocument(BaseModel):
    # Required fields
    id: str = Field(..., description="Unique identifier for the document")
    type: str = Field(..., description="Type of document (facetable)")

    # Multilingual text fields
    title_de: Optional[str] = Field(None)
    title_en: Optional[str] = Field(None)
    description_de: Optional[str] = Field(None)
    description_en: Optional[str] = Field(None)

    # Location fields
    location: Optional[str] = Field(None, description="Location name")
    location_type: Optional[LocationType] = Field(None)
    location_lat_lng: Optional[List[float]] = Field(None, description="Geographical coordinates [lat, lng]")

    # Media and links
    image_url: Optional[str] = Field(None, description="URL to associated image")
    link_locators: Optional[Dict[str, Any]] = Field(
        None, description="Locators for volunteering and other links, e.g., {'volunteering': '1234'}"
    )

    # User information
    user_full_name: Optional[str] = Field(
        None, description="Full name of associated user (searchable with infix and stemming, sortable)"
    )

    # Embeddings for search
    embedding: Optional[List[float]] = Field(
        None, description="Main embedding vector generated from title, description, and location fields"
    )

    # Topics and skills
    topics_v2: Optional[List[str]] = Field(None)
    skills_v2: Optional[List[str]] = Field(None)
    topics_skills_embedding_v2: Optional[List[float]] = Field(
        None, description="53-dimensional embedding for topics and skills"
    )
    similarity_embedding: Optional[List[float]] = Field(
        None, description="256-dimensional embedding for similarity matching"
    )

    # Organizer information
    organizer_name: Optional[str] = Field(None, description="Name of the organizer")
    organizer_image_url: Optional[str] = Field(None, description="URL to organizer's image")
    organizer_url: Optional[str] = Field(None, description="URL to organizer's profile or website")

    # External references
    external_url: Optional[str] = Field(None, description="External URL reference")
    source: Optional[str] = Field(None, description="Source of the content")

    # Commitment information
    commitment_label: Optional[CommitmentLabel] = Field(
        None, description="Label describing commitment level (facetable)"
    )
    commitment_duration: Optional[CommitmentDuration] = Field(None, description="Duration of commitment (facetable)")
    commitment_duration_ai_derived: Optional[bool] = Field(
        None, description="Whether commitment duration was derived by AI"
    )
    commitment_duration_ai_reasoning: Optional[str] = Field(
        None, description="AI reasoning for commitment duration derivation"
    )

    # Space and contact
    # TODO(SF) probably remove?
    space_id: Optional[str] = Field(None, description="Identifier for associated space")
    contact: Optional[List[ContactInfo]] = Field(None, description="List of contact information")

    # Donation fields
    donation_progress_percentage: Optional[int] = Field(
        None, description="Percentage of donation goal achieved", ge=0, le=100
    )
    donation_open_amount_in_cents: Optional[int] = Field(
        None, description="Remaining donation amount needed in cents", ge=0
    )
