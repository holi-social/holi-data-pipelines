"""
Commitment label mapping for volunteering opportunities.

Maps provider-specific commitment labels to standardized Holi values:
- one-time: Short-term, singular events
- recurring: Regular, ongoing commitments
- negotiable: Flexible arrangements

Priority order for multiple values: one-time > recurring > negotiable
"""

import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union


class CommitmentMapper(ABC):
    """Base commitment mapper interface using Strategy pattern."""

    @abstractmethod
    def can_handle(self, source: Optional[str]) -> bool:
        """Determine if this mapper can handle the given source."""
        pass

    @abstractmethod
    def extract_commitment(self, data: Any) -> Optional[str]:
        """Extract and map commitment label from source-specific data."""
        pass


class VostelCommitmentMapper(CommitmentMapper):
    """Commitment mapper for Vostel data."""

    FREQUENCY_MAPPING = {
        "one_time": "one-time",
        "project_based": "negotiable",
        "frequently": "recurring",
    }

    def can_handle(self, source: Optional[str]) -> bool:
        return source == "vostel.de"

    def extract_commitment(self, data: Any) -> Optional[str]:
        if not isinstance(data, dict):
            return None

        frequency = data.get("frequency")
        if not isinstance(frequency, str):
            return None

        return self.FREQUENCY_MAPPING.get(frequency)


class GoCommitmentMapper(CommitmentMapper):
    """Commitment mapper for GoVolunteer and GoNature data."""

    def can_handle(self, source: Optional[str]) -> bool:
        return source in ("GoVolunteer", "GoNature")

    def extract_commitment(self, data: Any) -> Optional[str]:
        if not isinstance(data, dict):
            return None

        expenditures = data.get("expenditures")
        if not isinstance(expenditures, (list, tuple)):
            return None

        return self._get_commitment_from_expenditures(expenditures)

    def _get_commitment_from_expenditures(self, expenditures: Union[List[str], Tuple[str, ...]]) -> str:
        if not expenditures:
            return "negotiable"

        values = [str(exp).lower() for exp in expenditures]

        if "einmalig" in values:
            return "one-time"
        if "flexibel" in values:
            return "negotiable"
        if any(term in values for term in ["regelmäßig", "regelmässig"]):
            return "recurring"

        return "negotiable"


class AktionMenschCommitmentMapper(CommitmentMapper):
    """Commitment mapper for Aktion Mensch data."""

    def can_handle(self, source: Optional[str]) -> bool:
        return source == "Aktion Mensch"

    def extract_commitment(self, data: Any) -> Optional[str]:
        if not data:
            return None

        # Handle attribute field
        attributes = None
        if hasattr(data, "attribute"):
            attributes = data.attribute
        elif isinstance(data, dict) and "attribute" in data:
            attributes = data["attribute"]

        # Handle different attribute formats (JSON type in DB, string, or list)
        if isinstance(attributes, str):
            try:
                attributes = json.loads(attributes)
            except json.JSONDecodeError:
                return None

        # Check if attributes is a valid list
        if not isinstance(attributes, list):
            return None

        # Extract commitment values
        commitment_values = []
        for attr in attributes:
            if isinstance(attr, dict) and attr.get("typ") == "regelmaessigkeit" and attr.get("name"):
                commitment_values.append(attr["name"].lower())

        if not commitment_values:
            return None

        if any(value == "einmalig" for value in commitment_values):
            return "one-time"

        if any(value in ["regelmäßig", "regelmässig"] for value in commitment_values):
            return "recurring"

        return "negotiable"


class CommitmentMapperFactory:
    """Factory for creating appropriate commitment mappers."""

    _mappers: List[CommitmentMapper] = [
        VostelCommitmentMapper(),
        GoCommitmentMapper(),
        AktionMenschCommitmentMapper(),
    ]

    @classmethod
    def get_mapper(cls, source: Optional[str]) -> Optional[CommitmentMapper]:
        for mapper in cls._mappers:
            if mapper.can_handle(source):
                return mapper
        return None

    @classmethod
    def register_mapper(cls, mapper: CommitmentMapper) -> None:
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


def extract_commitment_label_from_item(item: Any) -> Optional[str]:
    """
    Extract commitment label from a classification item.

    Handles different data schemas across sources using source-specific adapters.
    """

    source = getattr(item, "source", None)
    data = None

    if hasattr(item, "additional_data") and item.additional_data:
        data = _parse_additional_data(item.additional_data)

    elif source == "Aktion Mensch" and hasattr(item, "attribute"):
        attribute = item.attribute

        if isinstance(attribute, str):
            try:
                attribute = json.loads(attribute)
            except json.JSONDecodeError:
                return None

        data = {"attribute": attribute}

    if data is None:
        return None

    mapper = CommitmentMapperFactory.get_mapper(source)
    if mapper:
        return mapper.extract_commitment(data)

    return None
