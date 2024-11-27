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
from src.common.variables import variable_value, JsonType, secret_value, update_last_successful_ingestion
from src.common.utils import batches
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# Initialize logging
logger = prefect.logging.get_logger()

@task
def extract_changed_rows(table_name: str, last_complete_ingestion_timestamp: str, latest_partition_timestamp: str) -> List[Dict[str, Any]]:
    """Extract changed data from BigQuery."""
    logger.info(f"Extracting changed data from table: {table_name}")
    data = extract_changes_in_partitions(table_name=table_name, new_partition_timestamp=latest_partition_timestamp, old_partition_timestamp=last_complete_ingestion_timestamp)
    logger.info(f"Detected {len(data)} changed records from BigQuery")
    return data

@task
def transform_data(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform raw data by processing each row."""
    transformed = [transform_row(row) for row in rows]
    logger.info(f"Transformed {len(rows)} rows of data")
    return transformed

@task
def upsert_data(typesense_client, typesense_collection_name, new_or_updated_documents) -> bool:
    ingestion_failures = False
    for docs_batch, batch_count in batches(new_or_updated_documents, 100):
        failed_documents, success_count = index_documents_in_batches(
            typesense_client.client, docs_batch, batch_count, typesense_collection_name
        )
        if failed_documents:
            ingestion_failures = True
            logger.error(
                f"Batch {batch_count} processed with failures: {len(failed_documents)} failed items out of"
                f" {len(docs_batch)}."
            )
            logger.error(f"Failed items in batch {batch_count}: {failed_documents}")
        else:
            ##### After processing, update the last successful ingestion time
            logger.info(f"Batch {batch_count} successfully processed with {success_count} documents.")
    return ingestion_failures

@task
def delete_data(typesense_client, typesense_collection_name, document_ids_to_delete):
    # TODO error handling
    # TODO performance considerations https://typesense.org/docs/27.1/api/documents.html#delete-documents (batch size)
    typesense_client.client.collections[typesense_collection_name].documents.delete(
        {"filter_by": f"id:={','.join(document_ids_to_delete)}"}
    )
    logger.info(f"Deleted {len(document_ids_to_delete)} documents from Typesense.")

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
        "state": variable_value("bq_to_typesense_state"),
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
    state = config.get("state")

    last_complete_ingestion_timestamp = None
    if last_complete_ingestion_timestamp:
        last_complete_ingestion_timestamp = datetime.strptime(state.get("last_complete_ingestion_timestamp"), TIMESTAMP_FORMAT).date()

    latest_partition_timestamp = extract_latest_partition_timestamp_from_bigquery(
        config.get("volunteering_raw_deduped")
    ).date()

    if last_complete_ingestion_timestamp and last_complete_ingestion_timestamp >= latest_partition_timestamp:
        logger.info(f"Last ingestion @ {last_complete_ingestion_timestamp} !< {latest_partition_timestamp}. Skipping ingestion.")
        return

    change_data = extract_changed_rows(
        config.get("volunteering_raw_deduped"),
        last_complete_ingestion_timestamp.strftime("%Y-%m-%d"),
        latest_partition_timestamp.strftime("%Y-%m-%d"),
    )

    new_or_updated_records = [record for record in change_data if record["change_type"] == "new_or_updated"]
    document_ids_to_delete = [record["id"] for record in change_data if record["change_type"] == "deleted"]

    new_or_updated_documents = transform_data(new_or_updated_records)

    typesense_collection_name = extract_collection_name_from_schema(config["schema_path"])
    recreate_collection = last_complete_ingestion_timestamp is None
    if recreate_collection:
        typesense_client.drop_and_create_collection(config["schema_path"])

    upsert_failures = upsert_data(typesense_client, typesense_collection_name, new_or_updated_documents)

    if document_ids_to_delete:
        delete_data(typesense_client, typesense_collection_name, document_ids_to_delete)

    if not upsert_failures:
        update_last_successful_ingestion(datetime.now())

    if upsert_failures:
        return Failed(message="Some documents failed ingestion. Please see error log.")

def setup_pipeline():
    config = get_config()
    typesense_client = TypesenseClient(config["typesense_config"])
    return config, typesense_client


if __name__ == "__main__":
    volunteering_to_typesense_etl()
