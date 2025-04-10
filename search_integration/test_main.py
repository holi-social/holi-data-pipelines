import base64
import json
from unittest.mock import patch
from pytest import fail
from typing import Any

# noinspection PyPackageRequirements
from cloudevents.http.event import CloudEvent
from typesense.exceptions import ObjectNotFound, RequestUnauthorized

from main import process_event

attributes: Any = {
    "id": "12833783708309476",
    "time": "2024-12-07T16:21:48.022Z",
    "specversion": "1.0",
    "datacontenttype": "application/json",
    "type": "google.cloud.pubsub.topic.v1.messagePublished",
    "source": "//pubsub.googleapis.com/",
}

user_payload: Any = {
    "id": "149a3d29-a0da-4f52-8e1c-660376b7084b",
    "name": "Daniel holi team",
    "email": "daniel@holi.team",
    "identity": "danielholi.team",
    "avatar": "https://ik.imagekit.io/holi/_DEV_/avatar/149a3d29-a0da-4f52-8e1c-660376b7084b_yv31UW7lD0.jpg",
    "aboutMe": "Daniel about himself",
    "location": "Klitmøller, Denmark",
}

space_payload: Any = {
    "id": "149a3d29-a0da-4f52-8e1c-660376b7084b",
    "name": "Daniels Space",
    "slug": "daniels-space",
    "avatar": "https://ik.imagekit.io/holi/_DEV_/avatar/149a3d29-a0da-4f52-8e1c-660376b7084b_yv31UW7lD0.jpg",
    "description": "This is Daniels Space",
    "location": "Klitmøller, Denmark",
    "locationLatLng": {
        "latitude": 57.0393497,
        "longitude": 8.474686,
    },
}


def message_data(event_type: Any, event_version: Any, data: Any) -> Any:
    return {
        "message": {
            "@type": "type.googleapis.com/google.pubsub.v1.PubsubMessage",
            "data": base64.b64encode(json.dumps(data).encode("utf-8")),
            "attributes": {
                "eventType": event_type,
                "eventVersion": event_version,
            },
            "messageId": "12833783708309476",
            "publishTime": "2024-12-07T16:21:48.022Z",
        }
    }


@patch("typesense.client.Client")
def test_user_updated(mock_client: Any) -> None:
    process_event(mock_client, CloudEvent(attributes, message_data("UserUpdated", "1.1.0", {"user": user_payload})))
    mock_client.upsert.assert_called_with(
        {
            "id": f"profile_{user_payload['id']}",
            "type": "profile",
            "user_full_name": user_payload["name"],
            "location": user_payload["location"],
            "location_lat_lng": None,
            "image_url": user_payload["avatar"],
            "link_locators": {
                "profile": user_payload["id"],
            },
        }
    )


@patch("typesense.client.Client")
def test_ignores_deletes_user_without_name(mock_client: Any) -> None:
    # users without names must not show up in search results
    user = user_payload.copy()
    # noinspection PyTypeChecker
    del user["name"]
    process_event(mock_client, CloudEvent(attributes, message_data("UserUpdated", "1.1.0", {"user": user})))
    mock_client.upsert.assert_not_called()
    mock_client.delete.assert_called_with(f"profile_{user['id']}")

    user["name"] = ""
    process_event(mock_client, CloudEvent(attributes, message_data("UserUpdated", "1.1.0", {"user": user})))
    mock_client.upsert.assert_not_called()
    mock_client.delete.assert_called_with(f"profile_{user['id']}")


@patch("typesense.client.Client")
def test_ignores_404_on_deletion(mock_client: Any) -> None:
    mock_client.delete.side_effect = ObjectNotFound("Object not found")
    user = user_payload.copy()
    user["name"] = ""
    process_event(mock_client, CloudEvent(attributes, message_data("UserUpdated", "1.1.0", {"user": user})))
    mock_client.upsert.assert_not_called()
    mock_client.delete.assert_called_with(f"profile_{user['id']}")


@patch("typesense.client.Client")
def test_raises_others_error_responses_on_deletion(mock_client: Any) -> None:
    # obviously does not cover *all* other errors but one to check if not all exceptions are "swallowed"
    mock_client.delete.side_effect = RequestUnauthorized("Unauthorized")
    user = user_payload.copy()
    user["name"] = ""
    try:
        process_event(mock_client, CloudEvent(attributes, message_data("UserUpdated", "1.1.0", {"user": user})))
    except RequestUnauthorized:
        pass
    except Exception:
        fail("Unexpected exception raised:")


@patch("typesense.client.Client")
def test_user_name_updated_ignored(mock_client: Any) -> None:
    process_event(
        mock_client, CloudEvent(attributes, message_data("UserNameUpdated", "1.1.0", {"user": user_payload}))
    )
    mock_client.upsert.assert_not_called()


@patch("typesense.client.Client")
def test_user_email_updated_ignored(mock_client: Any) -> None:
    process_event(
        mock_client, CloudEvent(attributes, message_data("UserEmailUpdated", "1.1.0", {"user": user_payload}))
    )
    mock_client.upsert.assert_not_called()


@patch("typesense.client.Client")
def test_user_deleted(mock_client: Any) -> None:
    process_event(mock_client, CloudEvent(attributes, message_data("UserDeleted", "1.1.0", {"user": user_payload})))
    mock_client.delete.assert_called_with(f"profile_{user_payload['id']}")


# noinspection DuplicatedCode
@patch("typesense.client.Client")
def test_space_created(mock_client: Any) -> None:
    process_event(mock_client, CloudEvent(attributes, message_data("SpaceCreated", "1.1.0", {"space": space_payload})))
    mock_client.upsert.assert_called_with(
        {
            "id": f"space_{space_payload['id']}",
            "type": "space",
            "title_de": space_payload["name"],
            "title_en": space_payload["name"],
            "description_de": space_payload["description"],
            "description_en": space_payload["description"],
            "location": space_payload["location"],
            "location_lat_lng": [
                space_payload["locationLatLng"]["latitude"],
                space_payload["locationLatLng"]["longitude"],
            ],
            "image_url": space_payload["avatar"],
            "link_locators": {
                "space": space_payload["slug"],
            },
        }
    )


# noinspection DuplicatedCode
@patch("typesense.client.Client")
def test_space_updated(mock_client: Any) -> None:
    process_event(mock_client, CloudEvent(attributes, message_data("SpaceUpdated", "1.1.0", {"space": space_payload})))
    mock_client.upsert.assert_called_with(
        {
            "id": f"space_{space_payload['id']}",
            "type": "space",
            "title_de": space_payload["name"],
            "title_en": space_payload["name"],
            "description_de": space_payload["description"],
            "description_en": space_payload["description"],
            "location": space_payload["location"],
            "location_lat_lng": [
                space_payload["locationLatLng"]["latitude"],
                space_payload["locationLatLng"]["longitude"],
            ],
            "image_url": space_payload["avatar"],
            "link_locators": {
                "space": space_payload["slug"],
            },
        }
    )
