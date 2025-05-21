"""Unit tests for location type mapping functionality."""

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pytest

from src.pipelines.volunteering_classification.location_type_mapping import (
    LocationTypeMapper,
    VostelLocationTypeMapper,
    GoLocationTypeMapper,
    AktionMenschLocationTypeMapper,
    LocationTypeMapperFactory,
    extract_location_type_from_item,
)


@pytest.fixture
def mappers() -> Dict[str, LocationTypeMapper]:
    return {
        "vostel": VostelLocationTypeMapper(),
        "go": GoLocationTypeMapper(),
        "aktion_mensch": AktionMenschLocationTypeMapper(),
    }


@dataclass
class MockItem:
    """Mock for classification input items."""

    source: Optional[str] = None
    additionalData: Optional[str] = None
    verortung: Optional[str] = None


class TestVostelLocationTypeMapper:
    def test_can_handle(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["vostel"].can_handle("vostel.de")
        assert not mappers["vostel"].can_handle("GoVolunteer")
        assert not mappers["vostel"].can_handle(None)

    def test_extract_location_type_remote(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        data = {"location_type": "location_independent"}
        assert mappers["vostel"].extract_location_type(data) == "REMOTE"

    def test_extract_location_type_not_remote(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        data = {"location_type": "on_site"}
        assert mappers["vostel"].extract_location_type(data) is None

    def test_extract_location_type_invalid_data(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["vostel"].extract_location_type("not a dict") is None
        assert mappers["vostel"].extract_location_type({}) is None
        assert mappers["vostel"].extract_location_type({"location_type": 123}) is None


class TestGoLocationTypeMapper:
    def test_can_handle(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["go"].can_handle("GoVolunteer")
        assert mappers["go"].can_handle("GoNature")
        assert not mappers["go"].can_handle("vostel.de")
        assert not mappers["go"].can_handle(None)

    def test_extract_location_type_remote(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        # Test with category field
        data = {"category": "REMOTE"}
        assert mappers["go"].extract_location_type(data) == "REMOTE"

        # Test with isRemote field
        data = {"isRemote": True}
        assert mappers["go"].extract_location_type(data) == "REMOTE"

        # Test with both fields
        data = {"category": "REMOTE", "isRemote": True}
        assert mappers["go"].extract_location_type(data) == "REMOTE"

        # Test with conflicting values (should still return REMOTE)
        data = {"category": "ON_SITE", "isRemote": True}
        assert mappers["go"].extract_location_type(data) == "REMOTE"

    def test_extract_location_type_not_remote(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        # Neither field indicates remote
        data = {"category": "ON_SITE", "isRemote": False}
        assert mappers["go"].extract_location_type(data) is None

        # Only category present, not remote
        data = {"category": "ON_SITE"}
        assert mappers["go"].extract_location_type(data) is None

        # Only isRemote present, not remote
        data = {"isRemote": False}
        assert mappers["go"].extract_location_type(data) is None

    def test_extract_location_type_invalid_data(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["go"].extract_location_type("not a dict") is None
        assert mappers["go"].extract_location_type({}) is None
        assert mappers["go"].extract_location_type({"category": 123}) is None


class TestAktionMenschLocationTypeMapper:
    def test_can_handle(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["aktion_mensch"].can_handle("Aktion Mensch")
        assert not mappers["aktion_mensch"].can_handle("GoVolunteer")
        assert not mappers["aktion_mensch"].can_handle(None)

    def test_extract_location_type_remote(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        data = {"verortung": "ortsunabhaengig"}
        assert mappers["aktion_mensch"].extract_location_type(data) == "REMOTE"

    def test_extract_location_type_not_remote(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        data = {"verortung": "lokal"}
        assert mappers["aktion_mensch"].extract_location_type(data) is None

    def test_extract_location_type_invalid_data(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["aktion_mensch"].extract_location_type("not a dict") is None
        assert mappers["aktion_mensch"].extract_location_type({}) is None
        assert mappers["aktion_mensch"].extract_location_type({"verortung": 123}) is None


class TestLocationTypeMapperFactory:
    def test_get_mapper(self) -> None:
        assert isinstance(LocationTypeMapperFactory.get_mapper("vostel.de"), VostelLocationTypeMapper)
        assert isinstance(LocationTypeMapperFactory.get_mapper("GoVolunteer"), GoLocationTypeMapper)
        assert isinstance(LocationTypeMapperFactory.get_mapper("GoNature"), GoLocationTypeMapper)
        assert isinstance(LocationTypeMapperFactory.get_mapper("Aktion Mensch"), AktionMenschLocationTypeMapper)

        assert LocationTypeMapperFactory.get_mapper("unknown_source") is None
        assert LocationTypeMapperFactory.get_mapper(None) is None

    def test_register_mapper(self) -> None:
        class TestMapper(LocationTypeMapper):
            def can_handle(self, source: Optional[str]) -> bool:
                return source == "test_source"

            def extract_location_type(self, data: Any) -> Optional[str]:
                return "REMOTE"

        LocationTypeMapperFactory.register_mapper(TestMapper())
        mapper = LocationTypeMapperFactory.get_mapper("test_source")

        assert mapper is not None
        assert mapper.extract_location_type({}) == "REMOTE"


@pytest.mark.parametrize(
    "item,expected",
    [
        # Vostel tests
        (MockItem(source="vostel.de", additionalData='{"location_type": "location_independent"}'), "REMOTE"),
        (MockItem(source="vostel.de", additionalData='{"location_type": "on_site"}'), None),
        (MockItem(source="vostel.de", additionalData="{}"), None),
        # GoVolunteer tests
        (MockItem(source="GoVolunteer", additionalData='{"category": "REMOTE"}'), "REMOTE"),
        (MockItem(source="GoVolunteer", additionalData='{"category": "SOCIAL"}'), None),
        (MockItem(source="GoVolunteer", additionalData="{}"), None),
        # Aktion Mensch tests
        (MockItem(source="Aktion Mensch", verortung="ortsunabhaengig"), "REMOTE"),
        (MockItem(source="Aktion Mensch", verortung="lokal"), None),
        # Missing or invalid data
        (MockItem(source=None), None),
        (MockItem(source="unknown_source"), None),
        (MockItem(source="vostel.de"), None),
    ],
)
def test_extract_location_type_from_item(item: MockItem, expected: Optional[str]) -> None:
    assert extract_location_type_from_item(item) == expected
