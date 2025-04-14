import base64
import json
from typing import Any
from unittest.mock import patch

# noinspection PyPackageRequirements
from cloudevents.http.event import CloudEvent
from main import process_event
from pytest import fail

from typesense.exceptions import ObjectNotFound, RequestUnauthorized

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
    "topicsV2": ["animal-welfare", "arts-culture"],
}

geojson_point = {"type": "Point", "coordinates": [8.474686, 57.0393497]}

task_payload: Any = {
    "id": "149a3d29-a0da-4f52-8e1c-660376b7084c",
    "name": "Community Garden Clean-up",
    "description": "Help us clean up and prepare the community garden for the spring season. Tasks include weeding, planting new seedlings, and repairing garden beds.",
    "created": "2025-04-10T14:30:00",
    "timeframeStartDate": "2025-04-20",
    "timeframeEndDate": "2025-04-22",
    "regularity": "ONE_TIME",
    "duration": "HOURS_2to4",
    "slotsAvailable": 10,
    "slotsTaken": 3,
    "visibility": "PUBLIC",
    "locationType": "ONSITE",
    "location": "Klitmøller, Denmark",
    "geolocationGeoJson": geojson_point,
    "skillsV2": ["gardening", "carpentry", "landscaping"],
    "thumbnail": "https://example.com/images/garden-cleanup.jpg",
    "contactEmail": "garden@example.org",
    "contactPhone": "+49123456789",
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
    process_event(mock_client, CloudEvent(attributes, message_data("SpaceCreated", "1.2.0", {"space": space_payload})))
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
            "topics_v2": space_payload["topicsV2"],
        }
    )


# noinspection DuplicatedCode
@patch("typesense.client.Client")
def test_space_updated(mock_client: Any) -> None:
    process_event(mock_client, CloudEvent(attributes, message_data("SpaceUpdated", "1.2.0", {"space": space_payload})))
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
            "topics_v2": space_payload["topicsV2"],
        }
    )


@patch("typesense.client.Client")
def test_space_deleted(mock_client: Any) -> None:
    process_event(mock_client, CloudEvent(attributes, message_data("SpaceDeleted", "1.2.0", {"space": space_payload})))
    mock_client.delete.assert_called_with(f"space_{space_payload['id']}")


@patch("typesense.client.Client")
def test_task_created(mock_client: Any) -> None:
    process_event(
        mock_client,
        CloudEvent(
            attributes,
            message_data("TaskCreated", "1.0.0", {"user": user_payload, "space": space_payload, "task": task_payload}),
        ),
    )
    mock_client.upsert.assert_called_with(
        {
            "id": f"task_{task_payload['id']}",
            "type": "task",
            "title_de": task_payload["name"],
            "title_en": task_payload["name"],
            "description_de": task_payload["description"],
            "description_en": task_payload["description"],
            "location": task_payload["location"],
            "location_lat_lng": [
                task_payload["geolocationGeoJson"]["coordinates"][0],
                task_payload["geolocationGeoJson"]["coordinates"][1],
            ],
            "image_url": task_payload["thumbnail"],
            "link_locators": {"space": space_payload["slug"], "task": task_payload["id"]},
            "topics_v2": space_payload["topicsV2"],
            "skills_v2": task_payload["skillsV2"],
            "topics_skills_embedding_v2": None,  # TODO
            "similarity_embedding": None,  # TODO
        }
    )


@patch("typesense.client.Client")
def test_task_updated(mock_client: Any) -> None:
    process_event(
        mock_client,
        CloudEvent(
            attributes,
            message_data("TaskUpdated", "1.0.0", {"user": user_payload, "space": space_payload, "task": task_payload}),
        ),
    )
    mock_client.upsert.assert_called_with(
        {
            "id": f"task_{task_payload['id']}",
            "type": "task",
            "title_de": task_payload["name"],
            "title_en": task_payload["name"],
            "description_de": task_payload["description"],
            "description_en": task_payload["description"],
            "location": task_payload["location"],
            "location_lat_lng": [
                task_payload["geolocationGeoJson"]["coordinates"][0],
                task_payload["geolocationGeoJson"]["coordinates"][1],
            ],
            "image_url": task_payload["thumbnail"],
            "link_locators": {"space": space_payload["slug"], "task": task_payload["id"]},
            "topics_v2": space_payload["topicsV2"],
            "skills_v2": task_payload["skillsV2"],
            "topics_skills_embedding_v2": None,  # TODO
            "similarity_embedding": None,  # TODO
        }
    )


@patch("typesense.client.Client")
def test_task_deleted(mock_client: Any) -> None:
    process_event(
        mock_client,
        CloudEvent(
            attributes,
            message_data("TaskDeleted", "1.0.0", {"user": user_payload, "space": space_payload, "task": task_payload}),
        ),
    )
    mock_client.delete.assert_called_with(f"task_{task_payload['id']}")


@patch("typesense.client.Client")
def test_user_updated_with_missing_optional_fields(mock_client: Any) -> None:
    # Test with missing optional fields
    user = user_payload.copy()
    del user["avatar"]
    del user["aboutMe"]
    del user["location"]

    process_event(mock_client, CloudEvent(attributes, message_data("UserUpdated", "1.1.0", {"user": user})))
    mock_client.upsert.assert_called_with(
        {
            "id": f"profile_{user['id']}",
            "type": "profile",
            "user_full_name": user["name"],
            "location": None,
            "location_lat_lng": None,
            "image_url": None,
            "link_locators": {
                "profile": user["id"],
            },
        }
    )


@patch("typesense.client.Client")
def test_user_updated_with_wrong_version(mock_client: Any) -> None:
    # Test with unsupported version
    process_event(mock_client, CloudEvent(attributes, message_data("UserUpdated", "2.0.0", {"user": user_payload})))
    mock_client.upsert.assert_not_called()
    mock_client.delete.assert_not_called()


@patch("typesense.client.Client")
def test_space_updated_with_missing_location_data(mock_client: Any) -> None:
    # Test with missing location data
    space = space_payload.copy()
    del space["locationLatLng"]

    process_event(mock_client, CloudEvent(attributes, message_data("SpaceUpdated", "1.2.0", {"space": space})))
    mock_client.upsert.assert_called_with(
        {
            "id": f"space_{space['id']}",
            "type": "space",
            "title_de": space["name"],
            "title_en": space["name"],
            "description_de": space["description"],
            "description_en": space["description"],
            "location": space["location"],
            "location_lat_lng": None,
            "image_url": space["avatar"],
            "link_locators": {
                "space": space["slug"],
            },
            "topics_v2": space["topicsV2"],
        }
    )


@patch("typesense.client.Client")
def test_space_updated_with_empty_topics(mock_client: Any) -> None:
    # Test with empty topics list
    space = space_payload.copy()
    space["topicsV2"] = []

    process_event(mock_client, CloudEvent(attributes, message_data("SpaceUpdated", "1.2.0", {"space": space})))
    mock_client.upsert.assert_called_with(
        {
            "id": f"space_{space['id']}",
            "type": "space",
            "title_de": space["name"],
            "title_en": space["name"],
            "description_de": space["description"],
            "description_en": space["description"],
            "location": space["location"],
            "location_lat_lng": [
                space["locationLatLng"]["latitude"],
                space["locationLatLng"]["longitude"],
            ],
            "image_url": space["avatar"],
            "link_locators": {
                "space": space["slug"],
            },
            "topics_v2": [],
        }
    )


@patch("typesense.client.Client")
def test_space_updated_with_wrong_version(mock_client: Any) -> None:
    # Test with unsupported version (should be >=1.2.0)
    process_event(mock_client, CloudEvent(attributes, message_data("SpaceUpdated", "1.1.0", {"space": space_payload})))
    mock_client.upsert.assert_not_called()
    mock_client.delete.assert_not_called()


@patch("typesense.client.Client")
def test_task_updated_with_missing_optional_fields(mock_client: Any) -> None:
    # Test with missing optional fields
    task = task_payload.copy()
    del task["thumbnail"]
    del task["contactPhone"]
    del task["timeframeStartDate"]
    del task["timeframeEndDate"]
    del task["location"]
    del task["geolocationGeoJson"]
    del task["skillsV2"]

    process_event(
        mock_client,
        CloudEvent(
            attributes,
            message_data("TaskUpdated", "1.0.0", {"user": user_payload, "space": space_payload, "task": task}),
        ),
    )
    mock_client.upsert.assert_called_with(
        {
            "id": f"task_{task['id']}",
            "type": "task",
            "title_de": task["name"],
            "title_en": task["name"],
            "description_de": task["description"],
            "description_en": task["description"],
            "location": None,
            "location_lat_lng": None,
            "image_url": None,
            "link_locators": {"space": space_payload["slug"], "task": task_payload["id"]},
            "topics_v2": ["animal-welfare", "arts-culture"],
            "skills_v2": [],
            "topics_skills_embedding_v2": None,
            "similarity_embedding": None,
        }
    )


@patch("typesense.client.Client")
def test_task_updated_with_broken_geojson(mock_client: Any) -> None:
    # Test with missing optional fields
    task = task_payload.copy()
    del task["geolocationGeoJson"]["coordinates"]

    process_event(
        mock_client,
        CloudEvent(
            attributes,
            message_data("TaskUpdated", "1.0.0", {"user": user_payload, "space": space_payload, "task": task}),
        ),
    )
    mock_client.upsert.assert_called_with(
        {
            "id": f"task_{task['id']}",
            "type": "task",
            "title_de": task["name"],
            "title_en": task["name"],
            "description_de": task["description"],
            "description_en": task["description"],
            "location": task["location"],
            "location_lat_lng": None,
            "image_url": task_payload["thumbnail"],
            "link_locators": {"space": space_payload["slug"], "task": task_payload["id"]},
            "topics_v2": space_payload["topicsV2"],
            "skills_v2": task_payload["skillsV2"],
            "topics_skills_embedding_v2": None,
            "similarity_embedding": None,
        }
    )


@patch("typesense.client.Client")
def test_unknown_event_type(mock_client: Any) -> None:
    # Test with unknown event type
    process_event(
        mock_client, CloudEvent(attributes, message_data("UnknownEventType", "1.1.0", {"user": user_payload}))
    )
    mock_client.upsert.assert_not_called()
    mock_client.delete.assert_not_called()


@patch("typesense.client.Client")
def test_malformed_event_missing_attributes(mock_client: Any) -> None:
    # Test with missing event attributes
    malformed_attributes = attributes.copy()
    malformed_data = message_data("UserUpdated", "1.1.0", {"user": user_payload})
    del malformed_data["message"]["attributes"]["eventType"]

    # This should raise a KeyError due to missing eventType
    try:
        process_event(mock_client, CloudEvent(malformed_attributes, malformed_data))
        fail("Expected KeyError was not raised")
    except KeyError:
        pass

    mock_client.upsert.assert_not_called()
    mock_client.delete.assert_not_called()


@patch("typesense.client.Client")
def test_malformed_event_missing_data(mock_client: Any) -> None:
    # Test with missing data
    malformed_data = message_data("UserUpdated", "1.1.0", {})

    # This should raise a KeyError due to missing user data
    try:
        process_event(mock_client, CloudEvent(attributes, malformed_data))
        fail("Expected KeyError was not raised")
    except KeyError:
        pass

    mock_client.upsert.assert_not_called()
    mock_client.delete.assert_not_called()
