"""
Unit tests for commitment mapping functionality.

Tests mapper classes, factory pattern, and integration with source-specific schema handling.
"""

import json
from typing import Any, Dict, Optional

import pytest

from src.pipelines.volunteering_classification.commitment_labels_mapping import (
    CommitmentMapper,
    VostelCommitmentMapper,
    GoCommitmentMapper,
    AktionMenschCommitmentMapper,
    CommitmentMapperFactory,
    extract_commitment_label_from_item,
    _parse_additional_data,
)


@pytest.fixture
def mappers() -> Dict[str, CommitmentMapper]:
    return {
        "vostel": VostelCommitmentMapper(),
        "go": GoCommitmentMapper(),
        "aktion_mensch": AktionMenschCommitmentMapper(),
    }


class MockItem:
    def __init__(self, source: Optional[str], additional_data: Any = None, attribute: Any = None) -> None:
        self.source = source
        self.additional_data = additional_data
        if attribute is not None:
            self.attribute = attribute


@pytest.mark.parametrize(
    "mapper_key,source,expected",
    [
        # Vostel mapper
        ("vostel", "vostel.de", True),
        ("vostel", "GoVolunteer", False),
        ("vostel", None, False),
        # Go mapper
        ("go", "GoVolunteer", True),
        ("go", "GoNature", True),
        ("go", "vostel.de", False),
        ("go", None, False),
        # Aktion Mensch mapper
        ("aktion_mensch", "Aktion Mensch", True),
        ("aktion_mensch", "vostel.de", False),
        ("aktion_mensch", None, False),
    ],
)
def test_mapper_source_detection(
    mappers: Dict[str, CommitmentMapper], mapper_key: str, source: Optional[str], expected: bool
) -> None:
    assert mappers[mapper_key].can_handle(source) is expected


@pytest.mark.parametrize(
    "mapper_key,data,expected",
    [
        # Vostel format
        ("vostel", {"frequency": "one_time"}, "one-time"),
        ("vostel", {"frequency": "frequently"}, "recurring"),
        ("vostel", {"frequency": "project_based"}, "negotiable"),
        ("vostel", {"frequency": "invalid"}, None),
        ("vostel", {}, None),
        ("vostel", None, None),
        ("vostel", "not-a-dict", None),
        # Go format
        ("go", {"expenditures": ["Einmalig"]}, "one-time"),
        ("go", {"expenditures": ["Regelmäßig"]}, "recurring"),
        ("go", {"expenditures": ["regelmässig"]}, "recurring"),
        ("go", {"expenditures": ["Flexibel"]}, "negotiable"),
        ("go", {"expenditures": []}, "negotiable"),
        ("go", {"expenditures": ["Einmalig", "Regelmäßig"]}, "one-time"),
        ("go", {"expenditures": ["EINMALIG"]}, "one-time"),
        ("go", {"expenditures": ["Sonstiges"]}, "negotiable"),
        ("go", {}, None),
        ("go", None, None),
        ("go", {"expenditures": "not-a-list"}, None),
        # Aktion Mensch format
        ("aktion_mensch", {"attribute": [{"typ": "regelmaessigkeit", "name": "Einmalig"}]}, "one-time"),
        ("aktion_mensch", {"attribute": [{"typ": "regelmaessigkeit", "name": "Regelmäßig"}]}, "recurring"),
        ("aktion_mensch", {"attribute": [{"typ": "regelmaessigkeit", "name": "regelmässig"}]}, "recurring"),
        ("aktion_mensch", {"attribute": [{"typ": "regelmaessigkeit", "name": "Flexibel"}]}, "negotiable"),
        (
            "aktion_mensch",
            {
                "attribute": [
                    {"typ": "regelmaessigkeit", "name": "Einmalig"},
                    {"typ": "regelmaessigkeit", "name": "Regelmäßig"},
                ]
            },
            "one-time",
        ),
        (
            "aktion_mensch",
            {
                "attribute": [
                    {"typ": "other_type", "name": "Einmalig"},
                    {"typ": "regelmaessigkeit", "name": "Regelmäßig"},
                ]
            },
            "recurring",
        ),
        ("aktion_mensch", {"attribute": []}, None),
        ("aktion_mensch", {"attribute": [{"typ": "other_type", "name": "value"}]}, None),
        ("aktion_mensch", {"attribute": [{"typ": "regelmaessigkeit"}]}, None),
        ("aktion_mensch", {}, None),
        ("aktion_mensch", None, None),
        ("aktion_mensch", {"attribute": "not-a-list"}, None),
    ],
)
def test_mapper_extraction(
    mappers: Dict[str, CommitmentMapper], mapper_key: str, data: Any, expected: Optional[str]
) -> None:
    assert mappers[mapper_key].extract_commitment(data) == expected


def test_factory_get_mapper() -> None:
    assert isinstance(CommitmentMapperFactory.get_mapper("vostel.de"), VostelCommitmentMapper)
    assert isinstance(CommitmentMapperFactory.get_mapper("GoVolunteer"), GoCommitmentMapper)
    assert isinstance(CommitmentMapperFactory.get_mapper("GoNature"), GoCommitmentMapper)
    assert isinstance(CommitmentMapperFactory.get_mapper("Aktion Mensch"), AktionMenschCommitmentMapper)

    assert CommitmentMapperFactory.get_mapper("unknown_source") is None
    assert CommitmentMapperFactory.get_mapper(None) is None


def test_factory_register_mapper() -> None:
    class TestMapper(CommitmentMapper):
        def can_handle(self, source: Optional[str]) -> bool:
            return source == "test_source"

        def extract_commitment(self, data: Any) -> Optional[str]:
            return "test_commitment"

    CommitmentMapperFactory.register_mapper(TestMapper())
    mapper = CommitmentMapperFactory.get_mapper("test_source")

    assert mapper is not None
    assert mapper.extract_commitment({}) == "test_commitment"


@pytest.mark.parametrize(
    "input_data,expected",
    [
        # Valid data
        (json.dumps({"key": "value"}), {"key": "value"}),
        ({"already": "dict"}, {"already": "dict"}),
        # Invalid data
        ("", None),
        ("{}", None),
        ("null", None),
        ("{invalid json}", None),
        ([], None),
        ("[]", None),
        (123, None),
    ],
)
def test_parse_additional_data(input_data: Any, expected: Optional[Dict[str, Any]]) -> None:
    assert _parse_additional_data(input_data) == expected


@pytest.mark.parametrize(
    "item,expected",
    [
        # Vostel test with JSON string
        (MockItem(source="vostel.de", additional_data=json.dumps({"frequency": "one_time"})), "one-time"),
        # GoVolunteer test with JSON string
        (MockItem(source="GoVolunteer", additional_data=json.dumps({"expenditures": ["Einmalig"]})), "one-time"),
        # Aktion Mensch in additionalData
        (
            MockItem(
                source="Aktion Mensch",
                additional_data={"attribute": [{"typ": "regelmaessigkeit", "name": "Regelmäßig"}]},
            ),
            "recurring",
        ),
        # Unknown source
        (MockItem(source="UnknownSource", additional_data=json.dumps({"key": "value"})), None),
        # None source
        (MockItem(source=None, additional_data=json.dumps({"key": "value"})), None),
        # None additionalData
        (MockItem(source="vostel.de", additional_data=None), None),
    ],
)
def test_extract_commitment_from_item(item: MockItem, expected: Optional[str]) -> None:
    assert extract_commitment_label_from_item(item) == expected


def test_extract_from_aktion_mensch_direct_schema() -> None:
    class AktionMenschItem:
        def __init__(self) -> None:
            self.source = "Aktion Mensch"
            self.attribute = [{"typ": "regelmaessigkeit", "name": "Einmalig"}]

    item = AktionMenschItem()
    assert extract_commitment_label_from_item(item) == "one-time"


def test_extract_from_aktion_mensch_json_string_attribute() -> None:
    class AktionMenschJsonItem:
        def __init__(self) -> None:
            self.source = "Aktion Mensch"
            self.attribute = json.dumps([{"typ": "regelmaessigkeit", "name": "Einmalig"}])

    item = AktionMenschJsonItem()
    assert extract_commitment_label_from_item(item) == "one-time"
