from typing import List, Dict, Any, cast, Tuple

import prefect.logging
from prefect import task, flow, State
from prefect.states import Failed
from prefect.types import StrictVariableValue

from src.common.extractors.bq_extractor import extract_data_from_bigquery_table
from src.common.loaders.typesense_loader import index_documents_in_batches
from src.pipelines.volunteering_to_typesense.typesense_transformer import transform_row
from src.common.typesense_client import TypesenseClient
from src.common.variables import variable_value, secret_value
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
def load_data(documents: List[Dict[str, Any]], typesense_client: TypesenseClient, schema_path: str) -> None | State:
    """Load transformed data into Typesense in batches."""

    collection_name = typesense_client.drop_and_create_collection(schema_path)

    ingestion_failures = False
    for docs_batch, batch_count in batches(documents, 100):
        failed_documents, success_count = index_documents_in_batches(
            typesense_client.client, docs_batch, batch_count, collection_name
        )
        if failed_documents:
            ingestion_failures = True
            logger.error(
                f"Batch {batch_count} processed with failures: {len(failed_documents)} failed items out of"
                f" {success_count + len(failed_documents)}."
            )
            logger.error(f"Failed items in batch {batch_count}: {failed_documents}")
        else:
            logger.info(f"Batch {batch_count} successfully processed with {success_count} documents.")

    if ingestion_failures:
        return Failed(message="Some documents failed ingestion. Please see error log.")
    else:
        return None


def get_config() -> dict[str, StrictVariableValue]:
    return {
        "volunteering_with_classification": variable_value("volunteering_with_classification"),
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
    raw_data = extract_data(cast(str, config["volunteering_with_classification"]))
    transformed_data = transform_data(raw_data)
    load_data(transformed_data, typesense_client, cast(str, config["schema_path"]))


def setup_pipeline() -> Tuple[dict[str, StrictVariableValue], TypesenseClient]:
    config = get_config()
    typesense_client = TypesenseClient(cast(Dict[str, str], config["typesense_config"]))
    return config, typesense_client


if __name__ == "__main__":
    volunteering_to_typesense_etl()
