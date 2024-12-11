from typing import Iterable, Generator, Dict, cast, Tuple, Any, Optional
import itertools
from prefect.variables import Variable
import json
from prefect.types import StrictVariableValue

from src.common.typesense_client import TypesenseClient
from src.common.variables import variable_value, secret_value
from datetime import datetime, date
from src.pipelines.volunteering_to_typesense.tasks import delete_typesense_documents, upsert_typesense_documents, transform_data, extract_changed_records


def batches[T](iterable: Iterable[T], batch_size: int = 1) -> Generator[Tuple[Iterable[T], int], None, None]:
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


def setup_pipeline() -> Tuple[dict[str, StrictVariableValue], TypesenseClient]:
    config = get_config()
    typesense_client = TypesenseClient(cast(Dict[str, str], config["typesense_config"]))
    return config, typesense_client


def load_last_complete_ingestion_timestamp() -> datetime | None:
    state = cast(dict[str, Any], variable_value("volunteering_to_typesense_state"))
    timestamp_str = state.get("last_complete_ingestion_timestamp")
    if not timestamp_str:
        return None
    else:
        return datetime.fromisoformat(timestamp_str)


def store_last_complete_ingestion_timestamp(timestamp: datetime) -> None:
    """
    Store the last successful ingestion time in Prefect as a ISO 8601 string.
    """
    state = cast(dict[str, Any], variable_value("volunteering_to_typesense_state"))
    state["last_complete_ingestion_timestamp"] = timestamp.isoformat()
    Variable.set("volunteering_to_typesense_state", state, overwrite=True)


def diff_ingest(
    config: dict[str, StrictVariableValue],
    last_complete_ingestion_timestamp: Optional[date],
    latest_partition_timestamp: date,
    typesense_client: TypesenseClient,
    typesense_collection_name: str,
) -> Tuple[bool, bool]:
    """
    Incrementally ingests data into a Typesense collection by processing changed records.

    Args:
        config (dict[str, StrictVariableValue]): Configuration dictionary containing metadata.
        last_complete_ingestion_timestamp (date): Timestamp of the last successful ingestion, used
            to filter changed records.
        latest_partition_timestamp (date): The most recent timestamp to fetch updated data.
        typesense_client (TypesenseClient): Typesense client instance for interaction.
        typesense_collection_name (str): Name of the Typesense collection to update.

    Returns:
        Tuple[bool, bool]: A tuple indicating the success of deletions and upserts:
            - deletion_succeeded (bool): True if all deletions succeeded or no deletions were needed.
            - upsert_succeeded (bool): True if all new or updated documents were successfully upserted.

    """
    # Step 1: Extract changed records
    changed_records = extract_changed_records(
        cast(str, config.get("volunteering_raw_deduped")),
        last_complete_ingestion_timestamp.date() if last_complete_ingestion_timestamp is not None else None,
        latest_partition_timestamp,
    )

    # Step 2: Segregate records by change type
    new_or_updated_records = [
        record for record in changed_records if record["change_type"] == "new_or_updated"
    ]
    document_ids_to_delete = [
        record["id"] for record in changed_records if record["change_type"] == "deleted"
    ]

    # Step 3: Transform new or updated records
    new_or_updated_documents = transform_data(new_or_updated_records)

    # Step 4: Upsert documents into Typesense
    upsert_succeeded = upsert_typesense_documents(
        typesense_client, typesense_collection_name, new_or_updated_documents
    )

    # Step 5: Delete outdated documents from Typesense
    deletion_succeeded = True
    if document_ids_to_delete:
        deletion_succeeded = delete_typesense_documents(
            typesense_client, typesense_collection_name, document_ids_to_delete
        )

    return deletion_succeeded, upsert_succeeded
