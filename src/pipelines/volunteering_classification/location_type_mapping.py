"""
Location type mapping for volunteering opportunities.

Maps provider-specific location fields to standardized Holi values:
- REMOTE: For remote/virtual opportunities
"""

import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class LocationTypeMapper(ABC):
    """Base location type mapper interface using Strategy pattern."""

    @abstractmethod
    def can_handle(self, source: Optional[str]) -> bool:
        """Determine if this mapper can handle the given source."""
        pass

    @abstractmethod
    def extract_location_type(self, data: Any) -> Optional[str]:
        """Extract and map location type from source-specific data."""
        pass


class VostelLocationTypeMapper(LocationTypeMapper):
    """Location type mapper for Vostel data."""

    def can_handle(self, source: Optional[str]) -> bool:
        return source == "vostel.de"

    def extract_location_type(self, data: Any) -> Optional[str]:
        if not isinstance(data, dict):
            return None

        location_type = data.get("location_type")
        if not isinstance(location_type, str):
            return None

        if location_type == "location_independent":
            return "REMOTE"
        return None


class GoLocationTypeMapper(LocationTypeMapper):
    """Location type mapper for GoVolunteer and GoNature data."""

    def can_handle(self, source: Optional[str]) -> bool:
        return source in ("GoVolunteer", "GoNature")

    def extract_location_type(self, data: Any) -> Optional[str]:
        if not isinstance(data, dict):
            return None

        category = data.get("category")
        if isinstance(category, str) and category == "REMOTE":
            return "REMOTE"

        # Also check isRemote field
        is_remote = data.get("isRemote")
        if isinstance(is_remote, bool) and is_remote:
            return "REMOTE"

        return None


class AktionMenschLocationTypeMapper(LocationTypeMapper):
    """Location type mapper for Aktion Mensch data."""

    def can_handle(self, source: Optional[str]) -> bool:
        return source == "Aktion Mensch"

    def extract_location_type(self, data: Any) -> Optional[str]:
        if not data:
            return None

        # Handle verortung field
        verortung = None
        if hasattr(data, "verortung"):
            verortung = data.verortung
        elif isinstance(data, dict) and "verortung" in data:
            verortung = data["verortung"]

        if not isinstance(verortung, str):
            return None

        if verortung == "ortsunabhaengig":
            return "REMOTE"
        return None


class LocationTypeMapperFactory:
    """Factory for creating appropriate location type mappers."""

    _mappers: List[LocationTypeMapper] = [
        VostelLocationTypeMapper(),
        GoLocationTypeMapper(),
        AktionMenschLocationTypeMapper(),
    ]

    @classmethod
    def get_mapper(cls, source: Optional[str]) -> Optional[LocationTypeMapper]:
        for mapper in cls._mappers:
            if mapper.can_handle(source):
                return mapper
        return None

    @classmethod
    def register_mapper(cls, mapper: LocationTypeMapper) -> None:
        cls._mappers.append(mapper)


def _parse_additional_data(data: Any) -> Optional[Dict[str, Any]]:
    """Parse additionalData field into a dictionary."""
    if isinstance(data, str):
        if data.strip() in ["", "{}", "null"]:
            return None
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return None

    if not isinstance(data, dict):
        return None

    return data


def extract_location_type_from_item(item: Any) -> Optional[str]:
    """
    Extract location type from a classification item.

    Handles different data schemas across sources using source-specific adapters.
    """
    source = getattr(item, "source", None)
    data = None

    if hasattr(item, "additionalData") and item.additionalData:
        data = _parse_additional_data(item.additionalData)
    elif source == "Aktion Mensch" and hasattr(item, "verortung"):
        data = {"verortung": item.verortung}

    if data is None:
        return None

    mapper = LocationTypeMapperFactory.get_mapper(source)
    if mapper:
        return mapper.extract_location_type(data)

    return None
