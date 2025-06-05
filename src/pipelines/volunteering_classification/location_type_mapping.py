"""
Location type mapping for volunteering opportunities.

Maps provider-specific location fields to standardized Holi values:
- remote: For remote/virtual opportunities
- onsite: For in-person opportunities at specific locations
- unknown: When location type cannot be determined
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
            return "unknown"

        location_type = data.get("location_type")
        if not isinstance(location_type, str):
            return "unknown"

        if location_type == "location_independent":
            return "remote"
        elif location_type in ("location_dependent", "location_bound"):
            return "onsite"

        return "unknown"


class GoLocationTypeMapper(LocationTypeMapper):
    """Location type mapper for GoVolunteer and GoNature data."""

    def can_handle(self, source: Optional[str]) -> bool:
        return source in ("GoVolunteer", "GoNature")

    def extract_location_type(self, data: Any) -> Optional[str]:
        if not isinstance(data, dict):
            return "unknown"

        category = data.get("category")
        if isinstance(category, str) and category == "REMOTE":
            return "remote"
        elif isinstance(category, str) and category in ("ON_SITE", "ONSITE", "LOCAL"):
            return "onsite"

        is_remote = data.get("isRemote")
        if isinstance(is_remote, bool):
            return "remote" if is_remote else "onsite"
        elif isinstance(is_remote, str):
            if is_remote.lower() == "true":
                return "remote"
            elif is_remote.lower() == "false":
                return "onsite"

        return "unknown"


class AktionMenschLocationTypeMapper(LocationTypeMapper):
    """Location type mapper for Aktion Mensch data."""

    def can_handle(self, source: Optional[str]) -> bool:
        return source == "Aktion Mensch"

    def extract_location_type(self, data: Any) -> Optional[str]:
        if not data:
            return "unknown"

        verortung = None
        if hasattr(data, "verortung"):
            verortung = data.verortung
        elif isinstance(data, dict) and "verortung" in data:
            verortung = data["verortung"]

        if not isinstance(verortung, str):
            return "unknown"

        verortung_lower = verortung.lower()

        if verortung_lower == "ortsunabhaengig":
            return "remote"
        elif verortung_lower in ("durchfuehrende_organisation", "einsatzort"):
            return "onsite"

        return "unknown"


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

    if hasattr(item, "additional_data") and item.additional_data:
        data = _parse_additional_data(item.additional_data)
    elif source == "Aktion Mensch" and hasattr(item, "verortung"):
        data = {"verortung": item.verortung}

    if data is None:
        return None

    mapper = LocationTypeMapperFactory.get_mapper(source)
    if mapper:
        return mapper.extract_location_type(data)

    return None
