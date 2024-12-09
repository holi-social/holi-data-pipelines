from typing import List, Dict, Any

from prefect import task, State
from prefect.states import Failed, Completed
from src.common.extractors.bq_extractor import (
    extract_changes_in_partitions,
)
from src.common.loaders.typesense_loader import upsert_documents_in_batches
from src.pipelines.bq_to_typesense.typesense_transformer import transform_row
from src.common.typesense_client import TypesenseClient
from src.pipelines.bq_to_typesense.utils import batches
from datetime import date
import prefect.logging

logger = prefect.logging.get_logger()


@task
def extract_changed_records(
    table_name: str, last_complete_ingestion_date: date, latest_partition_date: date
) -> List[Dict[str, Any]]:
    """Extract changed data from BigQuery."""
    logger.info(f"Extracting changed data from table: {table_name}")
    data = extract_changes_in_partitions(
        table_name=table_name,
        new_partition_date=latest_partition_date,
        old_partition_date=last_complete_ingestion_date,
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
        failed_documents, success_count = upsert_documents_in_batches(
            client=typesense_client.client,
            docs=docs_batch,
            batch_num=batch_count,
            index_name=typesense_collection_name,
        )
        if failed_documents:
            ingestion_succeeded = False
            logger.error(
                f"Batch {batch_count} processed with failures: {len(failed_documents)} failed items out of"
                f" {success_count + len(failed_documents)}. Failed items in batch {batch_count}: {failed_documents}"
            )
        else:
            logger.info(f"Batch {batch_count} successfully upserted {success_count} documents.")
            logger.debug(f"Batch {batch_count} upserted document IDs: {[doc["id"] for doc in docs_batch]}")
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
        bool: True if deletion was successful, False otherwise
    """
    try:
        typesense_client.client.collections[typesense_collection_name].documents.delete(
            {"filter_by": f"id:={','.join([str(doc_id) for doc_id in document_ids_to_delete])}"}
        )
        logger.info(f"Deleted {len(document_ids_to_delete)} documents from Typesense.")
        return True
    except Exception as e:
        logger.error(
            f"Error deleting {len(document_ids_to_delete)} documents from Typesense: "
            + e.message
            + "\nFailed document ids: "
            + ",".join(document_ids_to_delete)
        )
        return False

@task
def validate_collection(typesense_client, collection_name: str, expected_count: int) -> State:
    """Validate the new collection before switching the alias."""
    # test a sample search
    sample_search = typesense_client.collections[collection_name].documents.search(
        {
            "q": "*",
            "query_by": "type",
            "page": 1,
            "per_page": 1,
        }
    )
    if not sample_search["hits"]:
        return Failed(message=f"No documents found for {collection_name}")

    return Completed()
