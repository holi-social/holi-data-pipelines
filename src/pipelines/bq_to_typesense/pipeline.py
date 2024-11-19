from typing import List, Dict, Any

import prefect.logging
from prefect import task, flow

from src.common.extractors.bq_extractor import extract_data_from_bigquery_table
from src.common.loaders.typesense_loader import index_documents_in_batches
from src.common.transformers.typesense_transformer import transform_row
from src.common.typesense_client import TypesenseClient
from src.common.variables import variable_value, JsonType

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


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx: min(ndx + n, l)]


@task
def load_data(documents: List[Dict[str, Any]], config: Dict[str, str]) -> None:
    """Load transformed data into Typesense in batches."""
    try:
        typesense_client_wrapper = TypesenseClient(config)
        ts_client = typesense_client_wrapper.get_client()

        index_name = typesense_client_wrapper.create_typesense_schema("typesense/all.json")
        # index_name = "staging_all"
        logger.info("Typesense schema created/updated successfully")

        batch_count = 0

        for docs_batch in batch(documents, 100):
            batch_count += 1
            # TODO handle returned result of index method
            index_documents_in_batches(ts_client, docs_batch, batch_count, index_name)
            logger.info(f"Indexed batch {batch_count} of size {len(docs_batch)}")
    except Exception as e:
        logger.error(f"Error loading data into Typesense: {e}")
        raise


def get_config() -> dict[str, JsonType]:
    return {
        "volunteering_with_classification": variable_value("volunteering_with_classification"),
        "typesense_config": {
            "api_key": variable_value("typesense_api_key"),  # FIXME move this to a secret block
            "host": variable_value("typesense_host"),
            "port": variable_value("typesense_port"),
            "protocol": variable_value("typesense_protocol")
        }
    }


# TODO configure flow via annotation
@flow()
def volunteering_to_typesense_etl() -> None:
    """Orchestrates the ETL pipeline to move data from BigQuery to Typesense."""
    try:
        config = get_config()
        raw_data = extract_data(config["volunteering_with_classification"])
        logger.info(f"first row: {raw_data[0]}")
        transformed_data = transform_data(raw_data)
        logger.info(f"first transformed row: {transformed_data[0]}")
        load_data(transformed_data, config=config)
        logger.info("ETL pipeline completed successfully")
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {e}")
        raise


if __name__ == "__main__":
    volunteering_to_typesense_etl()
    # volunteering_to_typesense_etl.serve(name="bq-to-typesense", cron="* * * * *")
