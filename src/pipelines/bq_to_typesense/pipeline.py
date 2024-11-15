import prefect.logging
from prefect import task, flow
from typing import List, Dict, Any

from prefect_gcp import BigQueryWarehouse

from src.common.extractors.bq_extractor import BigQueryExtractor
# from src.common.transformers.typesense_transformer import transform_row
# from src.common.loaders.typesense_loader import index_documents_in_batches
# from src.common.typesense_client import TypesenseClient
from src.config import get_config
# import logging

# Initialize logging
logger = prefect.logging.get_logger()

@task
def extract_data(config: Dict[str, str]) -> List[Dict[str, Any]]:
    """Extract data from BigQuery."""
    try:
        bq_extractor = BigQueryExtractor()
        volunteering_table = f"holi-shared.datapool_development.volunteering_voltastics_with_classification_enriched"
        logger.info(f"Extracting data from table: {volunteering_table}")
        data = bq_extractor.extract_data_from_bigquery(
            table_name=volunteering_table, return_format="dict_list"
        )
        logger.info(f"Extracted {len(data)} records from BigQuery")
        return data
    except Exception as e:
        logger.error(f"Error extracting data from BigQuery: {e}")
        raise


# @task
# def transform_data(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
#     """Transform raw data by processing each row."""
#     try:
#         transformed = [transform_row(row) for row in rows]
#         logger.info(f"Transformed {len(rows)} rows of data")
#         return transformed
#     except Exception as e:
#         logger.error(f"Error transforming data: {e}")
#         raise
#
#
# @task
# def load_data(documents: List[Dict[str, Any]], config: Dict[str, str]) -> None:
#     """Load transformed data into Typesense in batches."""
#     try:
#         ts_client = TypesenseClient(config=config)
#         ts_client.create_typesense_schema("typesense/all.json")
#         logger.info("Typesense schema created/updated successfully")
#
#         batch_size = 10000
#         batch = []
#         batch_count = 0
#
#         for doc in documents:
#             batch.append(doc)
#             if len(batch) >= batch_size:
#                 index_documents_in_batches(ts_client, batch, batch_count)
#                 logger.info(f"Indexed batch {batch_count} of size {len(batch)}")
#                 batch.clear()
#                 batch_count += 1
#
#         # Load any remaining documents
#         if batch:
#             index_documents_in_batches(ts_client, batch, batch_count)
#             logger.info(f"Indexed remaining batch {batch_count} of size {len(batch)}")
#     except Exception as e:
#         logger.error(f"Error loading data into Typesense: {e}")
#         raise


@flow
def volunteering_to_typesense_etl() -> None:
    """Orchestrates the ETL pipeline to move data from BigQuery to Typesense."""
    try:
        config = get_config()
        raw_data = extract_data(config)
        logger.info(f"first row: {raw_data[0]}")
        #       transformed_data = transform_data(raw_data)
        #       load_data(transformed_data, config=config)
        #       logger.info("ETL pipeline completed successfully")
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {e}")
        raise


if __name__ == "__main__":
    volunteering_to_typesense_etl()
    #volunteering_to_typesense_etl.serve(name="bq-to-typesense", cron="* * * * *")
