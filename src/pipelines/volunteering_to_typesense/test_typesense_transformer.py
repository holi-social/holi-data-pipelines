import random
import json
from typing import Any, List

import pytest
from faker import Faker

from src.pipelines.skills_v2 import skill_v2_slugs
from src.pipelines.topics_v2 import topic_v2_slugs
from src.pipelines.volunteering_classification.commitment_duration_classifier import (
    DURATIONS_CONSTANTS_TO_HUMAN_READABLE,
)
from src.pipelines.volunteering_to_typesense.typesense_transformer import transform_row

fake = Faker()

RowFixture = dict[str, Any]

TOPICS_SKILLS_EMBEDDING_V2_LENGTH = 53
SIMILARITY_EMBEDDING_LENGTH = 256


def _random(choices: List[Any]) -> Any:
    return random.choice(choices)  # noqa: S311 Standard pseudo-random generators are not suitable for cryptographic purposes


def _row() -> RowFixture:
    """Generates a random row with all fields set"""
    return {
        "id": fake.uuid4(),
        "title": fake.text(max_nb_chars=100),
        "description": fake.text(),
        "location": fake.city(),
        "latitude": str(fake.latitude()),
        "longitude": str(fake.longitude()),
        "image": fake.image_url(),
        "link": fake.url(),
        "orga": fake.company(),
        "imageOrga": fake.image_url(),
        "linkOrga": fake.image_url(),
        "source": fake.company(),
        "topics_skills_embedding_v2": [
            fake.pyfloat(min_value=-1, max_value=1) for i in range(TOPICS_SKILLS_EMBEDDING_V2_LENGTH)
        ],
        "topics_v2": [
            random.choice(topic_v2_slugs)  # noqa: S311
            for i in range(fake.random_int(min=0, max=len(topic_v2_slugs) - 1))
        ],
        "skills_v2": [
            random.choice(skill_v2_slugs)  # noqa: S311
            for i in range(fake.random_int(min=0, max=len(skill_v2_slugs) - 1))
        ],
        "similarity_embedding": [fake.pyfloat(min_value=-1, max_value=1) for i in range(SIMILARITY_EMBEDDING_LENGTH)],
        "location_type": _random([None, "REMOTE"]),
        "commitment_label": _random(
            [
                None,
                "ONE_TIME",
                "ONCE_WEEK",
                "ONCE_MONTH",
                "REGULARLY",
                "PROJECT_BASED",
                "SEVERAL_TIMES_WEEK",
                "NEGOTIABLE",
                "UNKNOWN",
            ]
        ),
        "commitment_duration": _random([None, *DURATIONS_CONSTANTS_TO_HUMAN_READABLE]),
        "commitment_duration_ai_reasoning": _random([None, "I hallucinate reasons"]),
        "contact": None,
    }


@pytest.fixture
def row() -> RowFixture:
    return _row()


def test_german_all_fields(row: RowFixture) -> None:
    row["title"] = "Dies ist ein deutscher Titel"
    row["description"] = "Dies ist eine deutsche Beschreibung"

    result = transform_row(row)

    assert result["id"] == f"volunteering_{row['id']}"
    assert result["type"] == "volunteering"
    assert result["title_de"] == row["title"]
    assert result["description_de"] == row["description"]
    assert result["location"] == row["location"]
    assert result["location_lat_lng"] == [row["latitude"], row["longitude"]]
    assert result["image_url"] == row["image"]
    assert result["link_locators"] == {"volunteering": row["id"]}
    assert result["external_url"] == row["link"]
    assert result["source"] == row["source"]
    assert result["organizer_name"] == row["orga"]
    assert result["organizer_image_url"] == row["imageOrga"]
    assert result["organizer_url"] == row["linkOrga"]

    assert result["topics_skills_embedding_v2"] == row["topics_skills_embedding_v2"]
    assert result["topics_v2"] == row["topics_v2"]
    assert result["skills_v2"] == row["skills_v2"]
    assert result["similarity_embedding"] == row["similarity_embedding"]
    assert result["location_type"] == row["location_type"]
    assert result["commitment_label"] == row["commitment_label"]
    assert result["commitment_duration"] == row["commitment_duration"]
    assert result["commitment_duration_ai_derived"] is True
    assert result["commitment_duration_ai_reasoning"] == row["commitment_duration_ai_reasoning"]
    assert result["contact"] == [{"type": "website", "value": row["link"], "label": row["orga"]}]


def test_english_title_and_description(row: RowFixture) -> None:
    row["title"] = "This is an english title"
    row["description"] = "This is an english description"

    result = transform_row(row)

    assert result["title_de"] is None
    assert result["title_en"] == row["title"]
    assert result["description_de"] is None
    assert result["description_en"] == row["description"]


def test_english_title_german_description(row: RowFixture) -> None:
    row["title"] = "This is an english title"
    row["description"] = "Dies ist eine deutsche Beschreibung"

    result = transform_row(row)

    assert result["title_de"] is None
    assert result["title_en"] == row["title"]
    assert result["description_de"] == row["description"]
    assert result["description_en"] is None


def test_german_title_english_description(row: RowFixture) -> None:
    row["title"] = "Dies ist ein deutscher Titel"
    row["description"] = "This is an english description"

    result = transform_row(row)

    assert result["title_de"] == row["title"]
    assert result["title_en"] is None
    assert result["description_de"] is None
    assert result["description_en"] == row["description"]


# noinspection PyTypedDict
def test_only_lat_lng_if_both_fields_set(row: RowFixture) -> None:
    result = transform_row(row)
    assert result["location_lat_lng"] == [row["latitude"], row["longitude"]]

    row["latitude"] = str(fake.latitude())
    row["longitude"] = None
    result = transform_row(row)
    assert result["location_lat_lng"] is None

    # AND
    row["latitude"] = None
    row["longitude"] = str(fake.longitude())
    result = transform_row(row)
    assert result["location_lat_lng"] is None


def test_serializability_of_row(row: RowFixture) -> None:
    json.dumps(transform_row(row))


# noinspection PyTypedDict
def test_id_mandatory(row: RowFixture) -> None:
    row["id"] = None

    with pytest.raises(ValueError) as e:
        transform_row(row)

    assert "id is mandatory" in str(e.value)


# noinspection PyTypedDict
def test_returns_none_for_invalid_lat(row: RowFixture) -> None:
    row["latitude"] = ""

    result = transform_row(row)

    assert result["location_lat_lng"] is None


# noinspection PyTypedDict
def test_returns_none_for_invalid_lng(row: RowFixture) -> None:
    row["longitude"] = ""

    result = transform_row(row)

    assert result["location_lat_lng"] is None


def test_requires_title(row: RowFixture) -> None:
    row_with_missing_title = {
        "id": row["id"],
        "title": None,
        "description": None,
        "location": None,
        "latitude": None,
        "longitude": None,
        "image": None,
        "link": None,
    }

    with pytest.raises(ValueError) as error:
        transform_row(row_with_missing_title)

    assert "title is mandatory" in str(error.value)


def test_falls_back_to_german_if_language_detection_on_title_fails(row: RowFixture) -> None:
    row["title"] = "lkajdclvoju9p3845798)&"

    result = transform_row(row)

    assert result["title_de"] == row["title"]


def test_falls_back_to_german_if_language_detection_on_description_fails(row: RowFixture) -> None:
    row["description"] = "lkajdclvoju9p3845798)&"

    result = transform_row(row)

    assert result["description_de"] == row["description"]


def test_falls_back_to_german_if_language_is_other(row: RowFixture) -> None:
    row["description"] = (
        "La ville de Paris, capitale de la France, est un joyau culturel qui fascine des millions de visiteurs chaque"
        " année."
    )

    result = transform_row(row)

    assert result["description_de"] == row["description"]


def test_full_contact_information(row: RowFixture) -> None:
    first_name = fake.first_name()
    last_name = fake.last_name()
    contact_person = f"{first_name} {last_name}"
    mobile = "+1 910-561-5313"
    phone = "(319)748-9241"
    email = fake.email()
    row["contact"] = json.dumps(
        {
            "anrede": fake.prefix(),
            "email": email,
            "id": fake.uuid4(),
            "mobil": mobile,
            "nachname": last_name,
            "telefon": phone,
            "titel": None,
            "vorname": first_name,
            "whatsapp": None,
        }
    )

    result = transform_row(row)

    assert result["contact"] == [
        {"type": "person", "value": contact_person},
        {"type": "email", "value": email},
        {"type": "phone", "value": "+1 910-561-5313"},
        {"type": "phone", "value": "+49 31974 89241"},
        {"type": "website", "value": row["link"], "label": row["orga"]},
    ]


def test_full_contact_information_dict(row: RowFixture) -> None:
    first_name = fake.first_name()
    last_name = fake.last_name()
    contact_person = f"{first_name} {last_name}"
    mobile = "+1 910-561-5313"
    phone = "(319)748-9241"
    email = fake.email()
    row["contact"] = {
        "anrede": fake.prefix(),
        "email": email,
        "id": fake.uuid4(),
        "mobil": mobile,
        "nachname": last_name,
        "telefon": phone,
        "titel": None,
        "vorname": first_name,
        "whatsapp": None,
    }

    result = transform_row(row)

    assert result["contact"] == [
        {"type": "person", "value": contact_person},
        {"type": "email", "value": email},
        {"type": "phone", "value": "+1 910-561-5313"},
        {"type": "phone", "value": "+49 31974 89241"},
        {"type": "website", "value": row["link"], "label": row["orga"]},
    ]


def test_falls_back_to_first_name_for_contact(row: RowFixture) -> None:
    first_name = fake.first_name()
    email = fake.email()
    row["contact"] = json.dumps(
        {
            "anrede": None,
            "email": email,
            "id": fake.uuid4(),
            "mobil": None,
            "nachname": None,
            "telefon": None,
            "titel": None,
            "vorname": first_name,
            "whatsapp": None,
        }
    )

    result = transform_row(row)

    assert result["contact"] == [
        {"type": "person", "value": first_name},
        {"type": "email", "value": email},
        {"type": "website", "value": row["link"], "label": row["orga"]},
    ]


def test_falls_back_to_last_name_for_contact(row: RowFixture) -> None:
    last_name = fake.last_name()
    email = fake.email()
    row["contact"] = json.dumps(
        {
            "anrede": None,
            "email": email,
            "id": fake.uuid4(),
            "mobil": None,
            "nachname": last_name,
            "telefon": None,
            "titel": None,
            "vorname": None,
            "whatsapp": None,
        }
    )

    result = transform_row(row)

    assert result["contact"] == [
        {"type": "person", "value": last_name},
        {"type": "email", "value": email},
        {"type": "website", "value": row["link"], "label": row["orga"]},
    ]


def test_handles_missing_contact_name(row: RowFixture) -> None:
    email = fake.email()
    row["contact"] = json.dumps(
        {
            "anrede": None,
            "email": email,
            "id": fake.uuid4(),
            "mobil": None,
            "nachname": None,
            "telefon": None,
            "titel": None,
            "vorname": None,
            "whatsapp": None,
        }
    )

    result = transform_row(row)

    assert result["contact"] == [
        {"type": "email", "value": email},
        {"type": "website", "value": row["link"], "label": row["orga"]},
    ]


def test_ignores_invalid_phone_numbers(row: RowFixture) -> None:
    first_name = fake.first_name()
    last_name = fake.last_name()
    contact_person = f"{first_name} {last_name}"
    mobile = "+1 910-561-5313"
    phone = "NOT A PROPER NUMBER"
    email = fake.email()
    row["contact"] = json.dumps(
        {
            "anrede": fake.prefix(),
            "email": email,
            "id": fake.uuid4(),
            "mobil": mobile,
            "nachname": last_name,
            "telefon": phone,
            "titel": None,
            "vorname": first_name,
            "whatsapp": None,
        }
    )

    result = transform_row(row)

    assert result["contact"] == [
        {"type": "person", "value": contact_person},
        {"type": "email", "value": email},
        {"type": "phone", "value": "+1 910-561-5313"},
        {"type": "website", "value": row["link"], "label": row["orga"]},
    ]


def test_keeps_phonenumbers_with_remarks(row: RowFixture) -> None:
    first_name = fake.first_name()
    last_name = fake.last_name()
    contact_person = f"{first_name} {last_name}"
    phone = "0123/1234567 (Di+Mi von 11-12 Uhr)"
    email = fake.email()
    row["contact"] = json.dumps(
        {
            "anrede": fake.prefix(),
            "email": email,
            "id": fake.uuid4(),
            "mobil": None,
            "nachname": last_name,
            "telefon": phone,
            "titel": None,
            "vorname": first_name,
            "whatsapp": None,
        }
    )

    result = transform_row(row)

    assert result["contact"] == [
        {"type": "person", "value": contact_person},
        {"type": "email", "value": email},
        {"type": "phone", "value": phone},
        {"type": "website", "value": row["link"], "label": row["orga"]},
    ]
