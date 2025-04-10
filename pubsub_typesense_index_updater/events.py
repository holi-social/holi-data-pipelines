import base64
import json
from dataclasses import dataclass
from typing import Dict, Any, Optional

# noinspection PyPackageRequirements
from cloudevents.http.event import CloudEvent


def get_event_type(cloud_event: CloudEvent) -> str:
    return cloud_event.data["message"]["attributes"]["eventType"]


def get_event_version(cloud_event: CloudEvent) -> str:
    return cloud_event.data["message"]["attributes"]["eventVersion"]


def _validate_event_type(cloud_event: CloudEvent, expected_event_type: str) -> None:
    event_type = get_event_type(cloud_event)
    if event_type != expected_event_type:
        raise ValueError(f"Expected event of type {expected_event_type}, got {event_type}")


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
class UserEvent:
    event_type: str
    event_version: str
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

    def __init__(self, data: Dict[str, Any]):
        self.id = data["space"].get("id")
        self.name = data["space"].get("name")
        self.slug = data["space"].get("slug")
        self.avatar = data["space"].get("avatar")
        self.description = data["space"].get("description")
        self.location = data["space"].get("location")
        contains_lat_lng = (
            data["space"].get("locationLatLng") is not None
            and data["space"].get("locationLatLng")["latitude"] is not None
            and data["space"].get("locationLatLng")["longitude"] is not None
        )
        self.location_lat_lng = (
            LatLng(
                data["space"].get("locationLatLng").get("latitude"),
                data["space"].get("locationLatLng").get("longitude"),
            )
            if contains_lat_lng
            else None
        )


@dataclass
class SpaceEvent:
    event_type: str
    event_version: str
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
        }


@dataclass
class SpaceCreatedEvent(SpaceEvent):
    def __init__(self, cloud_event: CloudEvent):
        _validate_event_type(cloud_event, "SpaceCreated")
        super().__init__(cloud_event)


@dataclass
class SpaceUpdatedEvent(SpaceEvent):
    def __init__(self, cloud_event: CloudEvent):
        _validate_event_type(cloud_event, "SpaceUpdated")
        super().__init__(cloud_event)


@dataclass
class SpaceDeletedEvent(SpaceEvent):
    def __init__(self, cloud_event: CloudEvent):
        _validate_event_type(cloud_event, "SpaceDeleted")
        super().__init__(cloud_event)
