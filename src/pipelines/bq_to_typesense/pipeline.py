import sys
from typing import List, Dict, Any, Union

import prefect.logging
from prefect import task, flow
from prefect.states import Failed

from src.common.extractors.bq_extractor import extract_data_from_bigquery_table
from src.common.loaders.typesense_loader import index_documents_in_batches
from src.pipelines.bq_to_typesense.typesense_transformer import transform_row
from src.common.typesense_client import TypesenseClient
from src.common.variables import variable_value, JsonType, secret_value
from src.common.utils import batches

# Initialize logging
logger = prefect.logging.get_logger()


@task
def extract_data(table_name: str) -> List[Dict[str, Any]]:
    """Extract data from BigQuery."""
    logger.info(f"Extracting data from table: {table_name}")
    data = extract_data_from_bigquery_table(table_name)
    logger.info(f"Extracted {len(data)} records from BigQuery")
    return data


@task
def transform_data(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform raw data by processing each row."""
    transformed = [transform_row(row) for row in rows]
    logger.info(f"Transformed {len(rows)} rows of data")
    return transformed


@task
def load_data(documents: List[Dict[str, Any]], typesense_client, schema_path: str) -> None | Failed:
    """Load transformed data into Typesense in batches."""

    collection_name = typesense_client.drop_and_create_collection(schema_path)

    ingestion_failures = False
    for (docs_batch, batch_count) in batches(documents, 100):
        failed_documents, success_count = index_documents_in_batches(typesense_client, docs_batch, batch_count, collection_name)
        if failed_documents:
            ingestion_failures = True
            logger.error(
                f"Batch {batch_count} processed with failures: {len(failed_documents)} failed items out of {len(docs_batch)}."
            )
            logger.error(f"Failed items in batch {batch_count}: {failed_documents}")
        else:
            logger.info(f"Batch {batch_count} successfully processed with {success_count} documents.")
    
    if ingestion_failures:
        return Failed(message="Some documents failed ingestion. Please see error log.")


def get_config() -> dict[str, JsonType]:
    return {
        "volunteering_with_classification": variable_value("volunteering_with_classification"),
        "typesense_config": {
            "api_key": secret_value("typesense-api-key"),
            "host": variable_value("typesense_host"),
            "port": variable_value("typesense_port"),
            "protocol": variable_value("typesense_protocol"),
        },
        "schema_path": "typesense/staging_all.json",
    }


# TODO configure flow via annotation
# TODO configure task so that they can be tried 
@flow()
def volunteering_to_typesense_etl() -> None:
    """Orchestrates the ETL pipeline to move volunteering data from BigQuery to Typesense."""
    config, typesense_client = setup_pipeline()
    raw_data = extract_data(config["volunteering_with_classification"])
    transformed_data = transform_data(raw_data)
    load_data(transformed_data, typesense_client, config["schema_path"])

def setup_pipeline():
    config = get_config()
    typesense_client = TypesenseClient(config["typesense_config"])
    return config,typesense_client


if __name__ == "__main__":
    volunteering_to_typesense_etl()
