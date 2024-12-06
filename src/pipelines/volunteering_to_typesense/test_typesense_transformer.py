import json

import pytest
from faker import Faker

from src.pipelines.volunteering_to_typesense.typesense_transformer import transform_row

fake = Faker()


@pytest.fixture
def row():
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
    }


def test_german_all_fields(row):
    row["title"] = "Dies ist ein deutscher Titel"
    row["description"] = "Dies ist eine deutsche Beschreibung"

    result = transform_row(row)

    assert result["type"] == "volunteering"
    assert result["volunteering_id"] == row["id"]
    assert result["title_de"] == row["title"]
    assert result["description_de"] == row["description"]
    assert result["location"] == row["location"]
    assert result["location_lat_lng"] == [row["latitude"], row["longitude"]]
    assert result["image_url"] == row["image"]
    assert result["link_locators"] == {"volunteering": row["id"]}


def test_english_title_and_description(row):
    row["title"] = "This is an english title"
    row["description"] = "This is an english description"

    result = transform_row(row)

    assert result["title_de"] is None
    assert result["title_en"] == row["title"]
    assert result["description_de"] is None
    assert result["description_en"] == row["description"]


def test_english_title_german_description(row):
    row["title"] = "This is an english title"
    row["description"] = "Dies ist eine deutsche Beschreibung"

    result = transform_row(row)

    assert result["title_de"] is None
    assert result["title_en"] == row["title"]
    assert result["description_de"] == row["description"]
    assert result["description_en"] is None


def test_german_title_english_description(row):
    row["title"] = "Dies ist ein deutscher Titel"
    row["description"] = "This is an english description"

    result = transform_row(row)

    assert result["title_de"] == row["title"]
    assert result["title_en"] is None
    assert result["description_de"] is None
    assert result["description_en"] == row["description"]


# noinspection PyTypedDict
def test_only_lat_lng_if_both_fields_set(row):
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


def test_serializability_of_row(row):
    json.dumps(transform_row(row))


# noinspection PyTypedDict
def test_id_mandatory(row):
    row["id"] = None

    with pytest.raises(ValueError) as e:
        transform_row(row)

    assert "id is mandatory" in str(e.value)


# noinspection PyTypedDict
def test_returns_none_for_invalid_lat(row):
    row["latitude"] = ""

    result = transform_row(row)

    assert result["location_lat_lng"] is None


# noinspection PyTypedDict
def test_returns_none_for_invalid_lng(row):
    row["longitude"] = ""

    result = transform_row(row)

    assert result["location_lat_lng"] is None


def test_tolerates_missing_values(row):

    row_with_missing_values = {
        "id": row["id"],
        "title": None,
        "description": None,
        "location": None,
        "latitude": None,
        "longitude": None,
        "image": None,
        "link": None,
    }

    result = transform_row(row_with_missing_values)

    assert result["type"] == "volunteering"
    assert result["volunteering_id"] == row_with_missing_values["id"]
    assert result["title_de"] is None
    assert result["title_en"] is None
    assert result["description_de"] is None
    assert result["description_en"] is None
    assert result["location"] is None
    assert result["location_lat_lng"] is None
    assert result["image_url"] is None
    assert result["link_locators"] == {"volunteering": row_with_missing_values["id"]}


def test_falls_back_to_german_if_language_detection_on_title_fails(row):
    row["title"] = "lkajdclvoju9p3845798)&"

    result = transform_row(row)

    assert result["title_de"] == row["title"]


def test_falls_back_to_german_if_language_detection_on_description_fails(row):
    row["description"] = "lkajdclvoju9p3845798)&"

    result = transform_row(row)

    assert result["description_de"] == row["description"]


def test_falls_back_to_german_if_language_is_other(row):
    row["description"] = (
        "La ville de Paris, capitale de la France, est un joyau culturel qui fascine des millions de visiteurs chaque"
        " année."
    )

    result = transform_row(row)

    assert result["description_de"] == row["description"]
