import json
from typing import Any, List, TypeVar, Optional

# T can be any type, but allows None as well
T = TypeVar("T")


def safe_get_attribute(item: Any, attr_name: str, default: Optional[T] = None) -> Optional[T]:
    """Get attribute with fallback to default if not present or None."""
    if hasattr(item, attr_name):
        value = getattr(item, attr_name)
        return value if value is not None else default
    return default


def parse_json_attribute(value: Any) -> Any:
    """Parse JSON string or return original value if parsing fails."""
    if not isinstance(value, str):
        return value

    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def extract_values_from_attributes(attributes: Any, attr_type: str) -> List[str]:
    """Extract values from Aktion Mensch attributes with specified type."""
    result: List[str] = []

    # Handle string attributes (JSON)
    attributes = parse_json_attribute(attributes)

    # Ensure we have a list
    if not isinstance(attributes, list):
        return result

    # Extract values of the specified type
    for attr in attributes:
        if isinstance(attr, dict) and attr.get("typ") == attr_type and attr.get("name"):
            name = attr.get("name")
            if name is not None:  # Ensure name is not None before adding
                result.append(str(name))

    return result
