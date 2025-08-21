import base64
import json
from dataclasses import dataclass
from typing import Dict, Any, Optional, Literal, List, TypedDict, Protocol
from datetime import datetime, date
from semver import Version

# noinspection PyPackageRequirements
from cloudevents.http.event import CloudEvent

from farm_fingerprint import farm_fingerprint
from shared.classifiers.classifier import ClassificationResult
from shared.events.event import Donation, DonationCreatedEvent, DonationUpdatedEvent, DonationDeletedEvent
from shared.mappings.task_mappings import (
    map_task_location_type_to_typesense_location_type,
    map_task_regularity_to_commitment_label,
)


class GeoJSONPoint(TypedDict):
    type: Literal["Point"]
    coordinates: List[float]  # longitude, latitude


def get_message_id(cloud_event: CloudEvent) -> str:
    return cloud_event.data["message"]["messageId"]


def get_event_type(cloud_event: CloudEvent) -> str:
    return cloud_event.data["message"]["attributes"]["eventType"]


def get_event_version(cloud_event: CloudEvent) -> Version:
    return Version.parse(cloud_event.data["message"]["attributes"]["eventVersion"])


def _validate_event_type(cloud_event: CloudEvent, expected_event_type: str) -> None:
    event_type = get_event_type(cloud_event)
    if event_type != expected_event_type:
        raise ValueError(f"Expected event of type {expected_event_type}, got {event_type}")


def _validate_event_version(cloud_event: CloudEvent, min_version: Version, max_version_excl: Version) -> None:
    event_version = get_event_version(cloud_event)
    if event_version < min_version or event_version >= max_version_excl:
        raise ValueError(
            f"Unexpected event version: min={min_version}, max_excl={max_version_excl}, actual={event_version}"
        )


class DomainEvent(Protocol):
    event_type: str
    event_version: Version


@dataclass
class UserPayload:
    id: str
    name: str
    email: str
    identity: str
    avatar: str
    about_me: str
    location: str

    def __init__(self, data: Dict[str, Any]) -> None:
        self.id = data["user"].get("id")
        self.name = data["user"].get("name")
        self.email = data["user"].get("email")
        self.identity = data["user"].get("identity")
        self.avatar = data["user"].get("avatar")
        self.about_me = data["user"].get("aboutMe")
        self.location = data["user"].get("location")


@dataclass
class UserEvent(DomainEvent):
    event_type: str
    event_version: Version
    user: UserPayload

    def __init__(self, cloud_event: CloudEvent) -> None:
        self.event_type = get_event_type(cloud_event)
        self.event_version = get_event_version(cloud_event)
        self.user = UserPayload(json.loads(base64.b64decode(cloud_event.data["message"]["data"])))

    def __str__(self) -> str:
        return f'{type(self).__name__}(id="{self.user.id}",name="{self.user.name}",email="***",identity="{self.user.identity}",avatar="{self.user.avatar}")'


@dataclass
class UserUpdatedEvent(UserEvent):
    def __init__(self, cloud_event: CloudEvent) -> None:
        _validate_event_type(cloud_event, "UserUpdated")
        _validate_event_version(cloud_event, Version(1, 0, 0), Version(2, 0, 0))
        super().__init__(cloud_event)

    def as_typesense_document(self) -> object:
        return {
            "id": f"profile_{self.user.id}",
            "type": "profile",
            "user_full_name": self.user.name,
            "location": self.user.location,
            "location_lat_lng": None,  # TODO maybe enrich?
            "image_url": self.user.avatar,
            "link_locators": {"profile": self.user.id},
        }


@dataclass
class UserDeletedEvent(UserEvent):
    def __init__(self, cloud_event: CloudEvent) -> None:
        _validate_event_type(cloud_event, "UserDeleted")
        _validate_event_version(cloud_event, Version(1, 0, 0), Version(2, 0, 0))
        super().__init__(cloud_event)


@dataclass
class LatLng:
    latitude: float
    longitude: float


@dataclass
class SpacePayload:
    id: str
    name: str
    slug: str
    avatar: str
    description: str
    location: str
    location_lat_lng: Optional[LatLng]
    topics_v2: Optional[List[str]]

    def __init__(self, data: Dict[str, Any]):
        self.id = data["space"].get("id")
        self.name = data["space"].get("name")
        self.slug = data["space"].get("slug")
        self.avatar = data["space"].get("avatar")
        self.description = data["space"].get("description")
        self.location = data["space"].get("location")
        contains_lat_lng = (
            data["space"].get("locationLatLng") is not None
            and data["space"].get("locationLatLng").get("latitude") is not None
            and data["space"].get("locationLatLng").get("longitude") is not None
        )
        self.location_lat_lng = (
            LatLng(
                data["space"].get("locationLatLng").get("latitude"),
                data["space"].get("locationLatLng").get("longitude"),
            )
            if contains_lat_lng
            else None
        )
        self.topics_v2 = data["space"].get("topicsV2")


@dataclass
class SpaceEvent(DomainEvent):
    event_type: str
    event_version: Version
    space: SpacePayload

    def __init__(self, cloud_event: CloudEvent):
        self.event_type = get_event_type(cloud_event)
        self.event_version = get_event_version(cloud_event)
        self.space = SpacePayload(json.loads(base64.b64decode(cloud_event.data["message"]["data"])))

    def as_typesense_document(self) -> object:
        return {
            "id": f"space_{self.space.id}",
            "type": "space",
            "title_de": self.space.name,
            "title_en": self.space.name,
            "description_de": self.space.description,
            "description_en": self.space.description,
            "location": self.space.location,
            "location_lat_lng": [self.space.location_lat_lng.latitude, self.space.location_lat_lng.longitude]
            if self.space.location_lat_lng is not None
            else None,
            "image_url": self.space.avatar,
            "link_locators": {"space": self.space.slug},
            "topics_v2": self.space.topics_v2 if self.event_version >= Version(1, 2, 0) else None,
            # TODO add reco embeddings
        }


@dataclass
class SpaceCreatedEvent(SpaceEvent):
    def __init__(self, cloud_event: CloudEvent):
        _validate_event_type(cloud_event, "SpaceCreated")
        _validate_event_version(cloud_event, Version(1, 1, 0), Version(2, 0, 0))
        super().__init__(cloud_event)


@dataclass
class SpaceUpdatedEvent(SpaceEvent):
    def __init__(self, cloud_event: CloudEvent):
        _validate_event_type(cloud_event, "SpaceUpdated")
        _validate_event_version(cloud_event, Version(1, 1, 0), Version(2, 0, 0))
        super().__init__(cloud_event)


@dataclass
class SpaceDeletedEvent(SpaceEvent):
    def __init__(self, cloud_event: CloudEvent):
        _validate_event_type(cloud_event, "SpaceDeleted")
        _validate_event_version(cloud_event, Version(1, 1, 0), Version(2, 0, 0))
        super().__init__(cloud_event)


@dataclass
class TaskPayload:
    id: str
    name: str
    description: str
    created: datetime
    timeframe_start_date: Optional[date]
    timeframe_end_date: Optional[date]
    regularity: str  # TODO or use Literal["REGULARLY", "PROJECT_BASED", "ONE_TIME", "ONCE_WEEK", "SEVERAL_TIMES_WEEK", "ONCE_MONTH", "NEGOTIABLE"]
    duration: str  # TODO or use Literal["MINUTES_10to15", "MINUTES_15to30", "MINUTES_30to60", "HOURS_1to2", "HOURS_2to4", "HOURS_4to6", "DAYS_1", "DAYS_2", "DAYS_3_OR_MORE", "INDETERMINATE"]
    slots_available: int
    slots_taken: int
    visibility: Literal["MEMBERS", "PUBLIC"]
    location_type: str  # TODO or use Literal["HYBRID", "REMOTE", "ONSITE"]
    location: Optional[str]
    geolocation_geojson: Optional[GeoJSONPoint]
    skills_v2: List[str]
    thumbnail: Optional[str]
    contact_email: str
    contact_phone: Optional[str]

    def __init__(self, data: Dict[str, Any]):
        task_data = data["task"]
        self.id = task_data.get("id")
        self.name = task_data.get("name")
        self.description = task_data.get("description")
        self.created = datetime.fromisoformat(task_data.get("created"))
        timeframe_start = task_data.get("timeframeStartDate")
        self.timeframe_start_date = date.fromisoformat(timeframe_start) if timeframe_start else None
        timeframe_end = task_data.get("timeframeEndDate")
        self.timeframe_end_date = date.fromisoformat(timeframe_end) if timeframe_end else None
        self.regularity = task_data.get("regularity")
        self.duration = task_data.get("duration")
        self.slots_available = task_data.get("slotsAvailable")
        self.slots_taken = task_data.get("slotsTaken")
        self.visibility = task_data.get("visibility")
        self.location_type = task_data.get("locationType")
        self.location = task_data.get("location")
        self.geolocation_geojson = task_data.get("geolocationGeoJson")
        self.skills_v2 = task_data.get("skillsV2", [])  # Default to empty list if None
        self.thumbnail = task_data.get("thumbnail")
        self.contact_email = task_data.get("contactEmail")
        self.contact_phone = task_data.get("contactPhone")

    def is_private(self) -> bool:
        return self.visibility.casefold() != "public"


@dataclass
class TaskEvent(DomainEvent):
    event_type: str
    event_version: Version
    user: UserPayload
    space: SpacePayload
    task: TaskPayload

    def __init__(self, cloud_event: CloudEvent):
        self.event_type = get_event_type(cloud_event)
        self.event_version = get_event_version(cloud_event)
        message_data = json.loads(base64.b64decode(cloud_event.data["message"]["data"]))
        self.user = UserPayload(message_data)
        self.space = SpacePayload(message_data)
        self.task = TaskPayload(message_data)

    def farm_fingerprint(self) -> str:
        # ATTENTION! This hash calculation needs to be kept in sync with the hash calculation defined in the
        # corresponding BigQuery views
        return farm_fingerprint(
            f"{self.task.name}|{self.task.description}|{self.task.location or ''}|{self.space.topics_v2 or []}|{self.task.skills_v2 or []}"
        )

    def as_typesense_document(self, classification: ClassificationResult | None) -> object:
        return {
            "id": f"task_{self.task.id}",
            "type": "task",
            "title_de": self.task.name,
            "title_en": self.task.name,
            "description_de": self.task.description,
            "description_en": self.task.description,
            "location": self.task.location,
            "location_lat_lng": [
                self.task.geolocation_geojson["coordinates"][0],
                self.task.geolocation_geojson["coordinates"][1],
            ]
            if self.task.geolocation_geojson and self.task.geolocation_geojson.get("coordinates")
            else None,
            "location_type": map_task_location_type_to_typesense_location_type(self.task.location_type),
            "commitment_label": map_task_regularity_to_commitment_label(self.task.regularity),
            "commitment_duration": self.task.duration,
            "image_url": self.task.thumbnail,
            "organizer_name": self.space.name,
            "organizer_image_url": self.space.avatar,
            "link_locators": {"space": self.space.slug, "task": self.task.id},
            "topics_v2": self.space.topics_v2,
            "skills_v2": self.task.skills_v2,
            "topics_skills_embedding_v2": classification.topics_skills_embedding_v2 if classification else None,
            "similarity_embedding": classification.similarity_embedding if classification else None,
        }


@dataclass
class TaskCreatedEvent(TaskEvent):
    def __init__(self, cloud_event: CloudEvent):
        _validate_event_type(cloud_event, "TaskCreated")
        _validate_event_version(cloud_event, Version(1, 0, 0), Version(2, 0, 0))
        super().__init__(cloud_event)


@dataclass
class TaskUpdatedEvent(TaskEvent):
    def __init__(self, cloud_event: CloudEvent):
        _validate_event_type(cloud_event, "TaskUpdated")
        _validate_event_version(cloud_event, Version(1, 0, 0), Version(2, 0, 0))
        super().__init__(cloud_event)


@dataclass
class TaskDeletedEvent(TaskEvent):
    def __init__(self, cloud_event: CloudEvent):
        _validate_event_type(cloud_event, "TaskDeleted")
        _validate_event_version(cloud_event, Version(1, 0, 0), Version(2, 0, 0))
        super().__init__(cloud_event)


def parse_donation_created_event(cloud_event: CloudEvent) -> DonationCreatedEvent:
    _validate_event_type(cloud_event, "DonationCreated")
    _validate_event_version(cloud_event, Version(1, 0, 0), Version(2, 0, 0))
    donation = Donation.model_validate_json(base64.b64decode(cloud_event.data["message"]["data"]))
    return DonationCreatedEvent(donation)


def parse_donation_updated_event(cloud_event: CloudEvent) -> DonationUpdatedEvent:
    _validate_event_type(cloud_event, "DonationUpdated")
    _validate_event_version(cloud_event, Version(1, 0, 0), Version(2, 0, 0))
    donation = Donation.model_validate_json(base64.b64decode(cloud_event.data["message"]["data"]))
    return DonationUpdatedEvent(donation)


def parse_donation_deleted_event(cloud_event: CloudEvent) -> DonationDeletedEvent:
    _validate_event_type(cloud_event, "DonationDeleted")
    _validate_event_version(cloud_event, Version(1, 0, 0), Version(2, 0, 0))
    donation = Donation.model_validate_json(base64.b64decode(cloud_event.data["message"]["data"]))
    return DonationDeletedEvent(donation)
