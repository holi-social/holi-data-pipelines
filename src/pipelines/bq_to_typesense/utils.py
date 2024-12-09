from typing import Iterable, Generator, Tuple
import itertools
from prefect.variables import Variable
import json
from prefect.types import StrictVariableValue

from src.common.typesense_client import TypesenseClient
from src.common.variables import variable_value, secret_value
from datetime import datetime, date
from tasks import delete_typesense_documents, upsert_typesense_documents, transform_data, extract_changed_records, validate_collection


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
    """Retrieve the current active collection for the given alias name.

    Args:
        typesense_client: The Typesense client instance.
        alias_name (str): The name of the alias to look up.

    Returns:
        str: The name of the collection associated with the alias, or an empty string if the alias does not exist.
    """
    try:
        alias_info = typesense_client.aliases[alias_name].retrieve()
        return alias_info.get("collection_name", "")
    except Exception as e:
        return ""

def diff_ingest(config, last_complete_ingestion_timestamp, latest_partition_timestamp, typesense_client,
                typesense_collection_name):

    changed_records = extract_changed_records(
        config.get("volunteering_raw_deduped"),
        last_complete_ingestion_timestamp,
        latest_partition_timestamp,
    )
    new_or_updated_records = [record for record in changed_records if record["change_type"] == "new_or_updated"]
    document_ids_to_delete = [record["id"] for record in changed_records if record["change_type"] == "deleted"]
    new_or_updated_documents = transform_data(new_or_updated_records)
    upsert_succeeded = upsert_typesense_documents(
        typesense_client, typesense_collection_name, new_or_updated_documents
    )
    deletion_succeeded = True
    if document_ids_to_delete:
        deletion_succeeded = delete_typesense_documents(
            typesense_client, typesense_collection_name, document_ids_to_delete
        )
    return deletion_succeeded, upsert_succeeded
