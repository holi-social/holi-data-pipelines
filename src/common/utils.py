from typing import Iterable, Generator, Tuple
import itertools
from prefect.variables import Variable
import json
from prefect.types import StrictVariableValue

from src.common.typesense_client import TypesenseClient
from src.common.variables import variable_value, secret_value
from datetime import datetime, date


def batches[T](iterable: Iterable[T], batch_size=1) -> Generator[Tuple[Iterable[T], int], None, None]:
    """Generator function yields batches of the original iterable in sizes of batch_size"""
    batch_number: int = 1
    for batch in itertools.batched(iterable, batch_size):
        yield batch, batch_number
        batch_number += 1


def extract_collection_name_from_schema(schema_path: str) -> str:
    with open(schema_path, "r") as file:
        schema = json.load(file)

    collection_name = schema.get("name")
    if not collection_name:
        raise ValueError("Schema is missing a 'name' field.")

    return collection_name


def get_config() -> dict[str, StrictVariableValue]:
    return {
        "volunteering_with_classification": variable_value("volunteering_with_classification"),
        "volunteering_raw_deduped": variable_value("volunteering_voltastics_raw_deduped"),
        "typesense_config": {
            "api_key": secret_value("typesense-admin-api-key"),
            "host": variable_value("typesense_host"),
            "port": variable_value("typesense_port"),
            "protocol": variable_value("typesense_protocol"),
        },
        "schema_path": f"typesense/{variable_value("prefect_environment")}_all.json",
    }


def setup_pipeline():
    config = get_config()
    typesense_client = TypesenseClient(config["typesense_config"])
    return config, typesense_client


def load_last_complete_ingestion_timestamp() -> datetime | None:
    state = variable_value("bq_to_typesense_state")
    timestamp_str = state.get("last_complete_ingestion_timestamp")
    if not timestamp_str:
        return None
    else:
        return datetime.fromisoformat(timestamp_str)


def store_last_complete_ingestion_timestamp(timestamp: datetime) -> None:
    """
    Store the last successful ingestion time in Prefect as a ISO 8601 string.
    """
    state = variable_value("bq_to_typesense_state")
    state["last_complete_ingestion_timestamp"] = timestamp.isoformat()
    Variable.set("bq_to_typesense_state", state, overwrite=True)


def get_active_collection(typesense_client, alias_name: str) -> str:
    """Retrieve the current active collection for the alias."""
    alias_info = typesense_client.aliases[alias_name].retrieve()
    return alias_info["collection_name"]


def validate_collection(typesense_client, collection_name: str, expected_count: int) -> bool:
    """Validate the new collection before switching the alias."""
    collection_info = typesense_client.collections[collection_name].retrieve()
    actual_count = collection_info["num_documents"]

    if actual_count < expected_count:
        return False

    # Additional health checks, e.g., test a sample search
    sample_search = typesense_client.collections[collection_name].documents.search(
        {
            "q": "*",
            "query_by": "type",
            "page": 1,
            "per_page": 1,
        }
    )
    if not sample_search["hits"]:
        return False

    return True
