from pickle import GLOBAL
from typing import List, Dict, Any
import prefect.logging
from prefect import task, flow, State
from prefect.states import Failed
import json

from src.common.extractors.bq_extractor import (
    extract_latest_partition_timestamp_from_bigquery,
    extract_changes_in_partitions,
)
from src.common.loaders.typesense_loader import index_documents_in_batches
from src.pipelines.bq_to_typesense.typesense_transformer import transform_row
from src.common.typesense_client import TypesenseClient
from src.common.variables import variable_value, JsonType, secret_value
from src.common.utils import batches
from datetime import datetime, date

# Initialize logging
logger = prefect.logging.get_logger()

@task
def extract_changed_records(table_name: str, last_complete_ingestion_date: date, latest_partition_date: date) -> List[Dict[str, Any]]:
    """Extract changed data from BigQuery."""
    logger.info(f"Extracting changed data from table: {table_name}")
    data = extract_changes_in_partitions(
        table_name=table_name,
        new_partition_date=latest_partition_date,
        old_partition_date=last_complete_ingestion_date
    )
    logger.info(f"Detected {len(data)} changed records from BigQuery")
    return data

@task
def transform_data(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform raw data by processing each row."""
    transformed = [transform_row(row) for row in rows]
    logger.info(f"Transformed {len(rows)} rows of data")
    return transformed

@task
def upsert_typesense_documents(typesense_client, typesense_collection_name, new_or_updated_documents) -> bool:
    ingestion_succeeded = True
    for docs_batch, batch_count in batches(new_or_updated_documents, 100):
        failed_documents, success_count = index_documents_in_batches(
            typesense_client.client, docs_batch, batch_count, typesense_collection_name
        )
        if failed_documents:
            ingestion_succeeded = False
            logger.error(
                f"Batch {batch_count} processed with failures: {len(failed_documents)} failed items out of"
                f" {len(docs_batch)}. Failed items in batch {batch_count}: {failed_documents}"
            )
        else:
            logger.info(f"Batch {batch_count} successfully processed with {success_count} documents.")
    return ingestion_succeeded

@task
def delete_typesense_documents(typesense_client, typesense_collection_name, document_ids_to_delete) -> bool:
    """
    Deletes documents with the given IDs from the Typesense collection.

    Args:
        typesense_client (TypesenseClient): Typesense client
        typesense_collection_name (str): name of the collection
        document_ids_to_delete (list(int)): IDs of documents to delete

    Returns:
        bool: True if deletion was successful
    """
    # TODO performance considerations https://typesense.org/docs/27.1/api/documents.html#delete-documents (batch size)
    try:
        typesense_client.client.collections[typesense_collection_name].documents.delete(
            {"filter_by": f"id:={','.join(document_ids_to_delete)}"}
        )
        logger.info(f"Deleted {len(document_ids_to_delete)} documents from Typesense.")
        return True
    except e:
        logger.error(f"Error deleting {len(document_ids_to_delete)} documents from Typesense: " + e.message + "\nFailed document ids: " + ','.join(document_ids_to_delete))
        return False


def extract_collection_name_from_schema(schema_path: str) -> str:
    with open(schema_path, "r") as file:
        schema = json.load(file)

    collection_name = schema.get("name")
    if not collection_name:
        raise ValueError("Schema is missing a 'name' field.")

    return collection_name

def get_config() -> dict[str, JsonType]:
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


# TODO configure flow via annotation
# TODO configure task so that they can be tried
@flow()
def volunteering_to_typesense_etl() -> None | State:
    """Orchestrates the ETL pipeline to move volunteering data from BigQuery to Typesense."""
    config, typesense_client = setup_pipeline()

    last_complete_ingestion_timestamp = load_last_complete_ingestion_timestamp()

    latest_partition_timestamp = extract_latest_partition_timestamp_from_bigquery(
        config.get("volunteering_raw_deduped")
    )

    # volunteering data is partitioned by day so we compare days only
    if last_complete_ingestion_timestamp and last_complete_ingestion_timestamp.date() >= latest_partition_timestamp.date():
        logger.info(f"Last complete ingestion ({last_complete_ingestion_timestamp}) ran on the same day as the newest partition ({latest_partition_timestamp}). Skipping ingestion.")
        return

    changed_records = extract_changed_records(
        config.get("volunteering_raw_deduped"),
        last_complete_ingestion_timestamp,
        latest_partition_timestamp,
    )

    new_or_updated_records = [record for record in changed_records if record["change_type"] == "new_or_updated"]
    document_ids_to_delete = [record["id"] for record in changed_records if record["change_type"] == "deleted"]

    new_or_updated_documents = transform_data(new_or_updated_records)

    typesense_collection_name = extract_collection_name_from_schema(config["schema_path"])
    recreate_collection = last_complete_ingestion_timestamp is None
    if recreate_collection:
        typesense_client.drop_and_create_collection(config["schema_path"])

    upsert_succeeded = upsert_typesense_documents(typesense_client, typesense_collection_name, new_or_updated_documents)

    deletion_succeeded = True
    if document_ids_to_delete:
        deletion_succeeded = delete_typesense_documents(typesense_client, typesense_collection_name, document_ids_to_delete)

    if upsert_succeeded and deletion_succeeded:
        store_last_complete_ingestion_timestamp(datetime.now())
    else:
        return Failed(message="Some documents failed ingestion. Please see error log.")

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
    state.set("last_complete_ingestion_timestamp", timestamp.isoformat())
    Variable.set("bq_to_typesense_state", state)

if __name__ == "__main__":
    volunteering_to_typesense_etl()
