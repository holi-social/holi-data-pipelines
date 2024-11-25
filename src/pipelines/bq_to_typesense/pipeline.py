from typing import List, Dict, Any
import prefect.logging
from prefect import task, flow, State
from prefect.states import Failed
import json

from src.common.extractors.bq_extractor import (
    extract_latest_partition_timestamp_from_bigquery,
    detect_changes_in_partitions,
)
from src.common.loaders.typesense_loader import index_documents_in_batches
from src.pipelines.bq_to_typesense.typesense_transformer import transform_row
from src.common.typesense_client import TypesenseClient
from src.common.variables import variable_value, JsonType, secret_value, update_last_successful_ingestion
from src.common.utils import batches
from datetime import datetime

# Initialize logging
logger = prefect.logging.get_logger()


@task
def extract_data(table_name: str, last_ingestion_timestamp: str) -> List[Dict[str, Any]]:
    """Extract data from BigQuery."""
    logger.info(f"Extracting data from table: {table_name}")
    data = detect_changes_in_partitions(view_name=table_name, last_ingestion_timestamp=last_ingestion_timestamp)
    logger.info(f"Detected {len(data)} changed records from BigQuery")
    return data


@task
def transform_data(
    rows: List[Dict[str, Any]], latest_partition_timestamp: str, last_ingestion_timestamp: str
) -> List[Dict[str, Any]]:
    """Transform raw data by processing each row."""
    transformed = [transform_row(row) for row in rows]
    logger.info(f"Transformed {len(rows)} rows of data")
    return transformed


@task
def load_data(
    typesense_client: TypesenseClient,
    new_or_updated_documents: List[Dict[str, Any]],
    document_ids_to_delete: List[str],
    schema_path: str,
    last_ingestion_timestamp: str,
) -> None | State:
    """Load transformed data into Typesense in batches."""

    if not last_ingestion_timestamp:
        collection_name = typesense_client.drop_and_create_collection(schema_path)
    else:
        with open(schema_path, "r") as file:
            schema = json.load(file)

        collection_name = schema.get("name")
        if not collection_name:
            raise ValueError("Schema is missing a 'name' field.")

    ingestion_failures = False
    for docs_batch, batch_count in batches(new_or_updated_documents, 100):
        failed_documents, success_count = index_documents_in_batches(
            typesense_client.client, docs_batch, batch_count, collection_name
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
            update_last_successful_ingestion(datetime.now())
            logger.info(f"Batch {batch_count} successfully processed with {success_count} documents.")

    if document_ids_to_delete:
        typesense_client.client.collections[collection_name].documents.delete(
            {"filter_by": f"id:={','.join(document_ids_to_delete)}"}
        )
        logger.info(f"Deleted {len(document_ids_to_delete)} documents from Typesense.")

    if ingestion_failures:
        return Failed(message="Some documents failed ingestion. Please see error log.")


def get_config() -> dict[str, JsonType]:
    return {
        "volunteering_with_classification": variable_value("volunteering_with_classification"),
        "volunteering_raw_deduped": variable_value("volunteering_voltastics_raw_deduped"),
        "last_ingestion_timestamp": variable_value("last_ingestion_timestamp"),
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
def volunteering_to_typesense_etl() -> None:
    """Orchestrates the ETL pipeline to move volunteering data from BigQuery to Typesense."""
    config, typesense_client = setup_pipeline()
    last_ingestion_timestamp = variable_value("last_ingestion_timestamp")
    if last_ingestion_timestamp:
        formatted_last_ingestion_timestamp = datetime.strptime(last_ingestion_timestamp, "%Y-%m-%d %H:%M:%S").date()

    partitiontime_column_name = "partitiontime"
    result = extract_latest_partition_timestamp_from_bigquery(
        config.get("volunteering_raw_deduped"), partitiontime_column_name
    )
    if not result:
        raise ValueError("No data returned from the query.")

    latest_partition_timestamp = result[0].get(partitiontime_column_name)
    if latest_partition_timestamp is None:
        raise ValueError(f"Partition column `{partitiontime_column_name}` returned no value.")

    formatted_latest_partition_timestamp = datetime.strptime(latest_partition_timestamp, "%Y-%m-%d %H:%M:%S %Z").date()

    if last_ingestion_timestamp and formatted_last_ingestion_timestamp >= formatted_latest_partition_timestamp:
        logger.info("No new data to process. Skipping ingestion.")
        return

    change_data = extract_data(
        config.get("volunteering_raw_deduped"),
        formatted_last_ingestion_timestamp.strftime("%Y-%m-%d"),
    )

    new_or_updated_records = [record for record in change_data if record["change_type"] == "new_or_updated"]
    deleted_records = [record for record in change_data if record["change_type"] == "deleted"]

    transformed_data = transform_data(new_or_updated_records)

    document_ids_to_delete = [record["id"] for record in deleted_records]
    load_data(
        typesense_client, transformed_data, document_ids_to_delete, config["schema_path"], last_ingestion_timestamp
    )


def setup_pipeline():
    config = get_config()
    typesense_client = TypesenseClient(config["typesense_config"])
    return config, typesense_client


if __name__ == "__main__":
    volunteering_to_typesense_etl()
