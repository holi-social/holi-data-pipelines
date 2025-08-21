from datetime import datetime
from typing import Optional, List, Generic, TypeVar
from semver import Version
from pydantic import BaseModel, ConfigDict, PlainValidator, PlainSerializer
from typing_extensions import Annotated


class DonationLink(BaseModel):
    templated: Optional[bool] = None
    href: Optional[str] = None
    rel: Optional[str] = None


class DonationPicture(BaseModel):
    links: List[DonationLink] = []
    fallback: Optional[bool] = None


class DonationProfilePicture(BaseModel):
    fallback: Optional[bool] = None
    links: List[DonationLink] = []


class DonationContact(BaseModel):
    links: List[DonationLink] = []
    picture: Optional[DonationPicture] = None
    name: Optional[str] = None
    id: Optional[int] = None


class DonationCarrier(BaseModel):
    links: List[DonationLink] = []
    country: Optional[str] = None
    picture: Optional[DonationPicture] = None
    id: Optional[int] = None
    city: Optional[str] = None
    name: Optional[str] = None


class DonationActiveMatchingFund(BaseModel):
    links: List[DonationLink] = []
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    activated_at: Optional[datetime] = None
    title: Optional[str] = None
    description: Optional[str] = None
    company_name: Optional[str] = None
    client_id: Optional[str] = None
    provided_amount_in_cents: Optional[int] = None
    donated_amount_in_cents: Optional[int] = None
    state: Optional[str] = None
    logo_url: Optional[str] = None
    maximum_matching_amount_in_cents: Optional[int] = None


class DonationMatchingEvent(BaseModel):
    banner_text_color: Optional[str] = None
    banner_background_color: Optional[str] = None
    links: List[str] = []
    matching_percentage: Optional[int] = None
    id: Optional[int] = None


class DonationCategory(BaseModel):
    slug: Optional[str] = None
    name: Optional[str] = None
    id: Optional[int] = None


class Donation(BaseModel):
    model_config = ConfigDict(extra="ignore")

    links: List[DonationLink] = []
    profile_picture: Optional[DonationProfilePicture] = None
    contact: Optional[DonationContact] = None
    completed_need_count: Optional[int] = None
    comments_count: Optional[int] = None
    donor_count: Optional[int] = None
    donations_count: Optional[int] = None
    negative_opinions_count: Optional[int] = None
    positive_opinions_count: Optional[int] = None
    blog_post_count: Optional[int] = None
    newsletter_subscriptions_count: Optional[int] = None
    incomplete_need_count: Optional[int] = None
    carrier: Optional[DonationCarrier] = None
    id: Optional[int] = None
    title: Optional[str] = None
    description: Optional[str] = None
    summary: Optional[str] = None
    hash: Optional[str] = None
    open_amount_in_cents: Optional[int] = None
    donated_amount_in_cents: Optional[int] = None
    tax_deductible: Optional[bool] = None
    donations_prohibited: Optional[bool] = None
    progress_percentage: Optional[int] = None
    street: Optional[str] = None
    city: Optional[str] = None
    zip: Optional[str] = None
    country: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    content_updated_at: Optional[datetime] = None
    activated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    active_matching_fund: Optional[DonationActiveMatchingFund] = None
    matching_events: List[DonationMatchingEvent] = []
    categories: List[DonationCategory] = []

SemverVersion = Annotated[
    Version,
    PlainValidator(lambda v: Version.parse(v) if isinstance(v, str) else v),
    PlainSerializer(str, return_type=str),
]

EventPayloadT = TypeVar('EventPayloadT', bound=BaseModel)

class Event(Generic[EventPayloadT], BaseModel):
    event_type: str
    event_version: SemverVersion
    data: EventPayloadT


class DonationCreatedEvent(Event[Donation]):
    def __init__(self, donation: Donation):
        super().__init__(event_type="DonationCreated", event_version=Version(1, 0, 0), data=donation)


class DonationUpdatedEvent(Event[Donation]):
    def __init__(self, donation: Donation):
        super().__init__(event_type="DonationUpdated", event_version=Version(1, 0, 0), data=donation)


class DonationDeletedEvent(Event[Donation]):
    def __init__(self, donation: Donation):
        super().__init__(event_type="DonationDeleted", event_version=Version(1, 0, 0), data=donation)
