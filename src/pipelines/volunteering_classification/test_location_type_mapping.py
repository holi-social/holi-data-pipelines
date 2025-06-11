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
    """Mock item for testing extract_location_type_from_item."""

    id: str = "123"
    source: Optional[str] = None
    additional_data: Optional[str] = None
    verortung: Optional[str] = None


class TestVostelLocationTypeMapper:
    def test_can_handle(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["vostel"].can_handle("vostel.de")
        assert not mappers["vostel"].can_handle("GoVolunteer")
        assert not mappers["vostel"].can_handle(None)

    def test_extract_location_type_remote(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        data: Dict[str, Any] = {"location_type": "location_independent"}
        assert mappers["vostel"].extract_location_type(data) == "remote"

    def test_extract_location_type_onsite(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        data: Dict[str, Any] = {"location_type": "location_dependent"}
        assert mappers["vostel"].extract_location_type(data) == "onsite"

    def test_extract_location_type_invalid_data(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["vostel"].extract_location_type("not a dict") == "unknown"
        assert mappers["vostel"].extract_location_type({}) == "unknown"
        assert mappers["vostel"].extract_location_type({"location_type": 123}) == "unknown"


class TestGoLocationTypeMapper:
    def test_can_handle(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["go"].can_handle("GoVolunteer")
        assert mappers["go"].can_handle("GoNature")
        assert not mappers["go"].can_handle("vostel.de")
        assert not mappers["go"].can_handle(None)

    def test_extract_location_type_remote(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        data: Dict[str, Any] = {"category": "REMOTE"}
        assert mappers["go"].extract_location_type(data) == "remote"

        data = {"isRemote": True}
        assert mappers["go"].extract_location_type(data) == "remote"

        data = {"category": "REMOTE", "isRemote": True}
        assert mappers["go"].extract_location_type(data) == "remote"

        data = {"category": "ON_SITE", "isRemote": True}
        assert mappers["go"].extract_location_type(data) == "onsite"

    def test_extract_location_type_onsite(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        data: Dict[str, Any] = {"category": "ON_SITE", "isRemote": False}
        assert mappers["go"].extract_location_type(data) == "onsite"

        data = {"category": "ON_SITE"}
        assert mappers["go"].extract_location_type(data) == "onsite"

        data = {"isRemote": False}
        assert mappers["go"].extract_location_type(data) == "onsite"

    def test_extract_location_type_invalid_data(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["go"].extract_location_type("not a dict") == "unknown"
        assert mappers["go"].extract_location_type({}) == "unknown"
        assert mappers["go"].extract_location_type({"category": 123}) == "unknown"


class TestAktionMenschLocationTypeMapper:
    def test_can_handle(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["aktion_mensch"].can_handle("Aktion Mensch")
        assert not mappers["aktion_mensch"].can_handle("GoVolunteer")
        assert not mappers["aktion_mensch"].can_handle(None)

    def test_extract_location_type_remote(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        data = {"verortung": "ortsunabhaengig"}
        assert mappers["aktion_mensch"].extract_location_type(data) == "remote"

    def test_extract_location_type_onsite(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        data = {"verortung": "durchfuehrende_organisation"}
        assert mappers["aktion_mensch"].extract_location_type(data) == "onsite"

        data = {"verortung": "einsatzort"}
        assert mappers["aktion_mensch"].extract_location_type(data) == "onsite"

    def test_extract_location_type_invalid_data(self, mappers: Dict[str, LocationTypeMapper]) -> None:
        assert mappers["aktion_mensch"].extract_location_type("not a dict") == "unknown"
        assert mappers["aktion_mensch"].extract_location_type({}) == "unknown"
        assert mappers["aktion_mensch"].extract_location_type({"verortung": 123}) == "unknown"


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
                return "remote"

        LocationTypeMapperFactory.register_mapper(TestMapper())
        mapper = LocationTypeMapperFactory.get_mapper("test_source")

        assert mapper is not None
        assert mapper.extract_location_type({}) == "remote"


@pytest.mark.parametrize(
    "item,expected",
    [
        # Vostel tests
        (MockItem(source="vostel.de", additional_data='{"location_type": "location_independent"}'), "remote"),
        (MockItem(source="vostel.de", additional_data='{"location_type": "location_dependent"}'), "onsite"),
        (MockItem(source="vostel.de", additional_data="{}"), None),
        # GoVolunteer tests
        (MockItem(source="GoVolunteer", additional_data='{"category": "REMOTE"}'), "remote"),
        (MockItem(source="GoVolunteer", additional_data='{"category": "ON_SITE"}'), "onsite"),
        (MockItem(source="GoVolunteer", additional_data="{}"), None),
        # Aktion Mensch tests
        (MockItem(source="Aktion Mensch", verortung="ortsunabhaengig"), "remote"),
        (MockItem(source="Aktion Mensch", verortung="durchfuehrende_organisation"), "onsite"),
        (MockItem(source="Aktion Mensch", verortung="einsatzort"), "onsite"),
        (MockItem(source=None), None),
        (MockItem(source="unknown_source"), None),
        (MockItem(source="vostel.de"), None),
    ],
)
def test_extract_location_type_from_item(item: MockItem, expected: Optional[str]) -> None:
    assert extract_location_type_from_item(item) == expected
