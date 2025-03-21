import json
from typing import Any

from src.common.variables import variable_value


def extract_collection_name_from_schema(schema_path: str) -> str:
    with open(schema_path, "r") as file:
        schema = json.load(file)

    collection_name = schema.get("name")
    if not collection_name:
        raise ValueError("Schema is missing a 'name' field.")

    return collection_name


def get_config() -> dict[str, Any]:
    return {"volunteering_raw_deduped": variable_value("volunteering_voltastics_raw_deduped")}
