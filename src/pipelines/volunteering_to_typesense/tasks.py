from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, List, Dict, Optional, cast, Tuple
from urllib.parse import quote

from prefect import task, State
from prefect.states import Failed, Completed
from prefect.variables import Variable
from prefect.cache_policies import NONE

from src.common.extractors.bq_extractor import (
    extract_upserted_records_in_partitions,
    extract_deleted_records_in_partitions,
)
from src.common.loaders.typesense_loader import upsert_typesense_documents
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.typesense_client import TypesenseClient
from src.common.pubsub_client import PubSubClient
from src.common.variables import variable_value
from src.pipelines.constants import LAST_INGESTION_TIMESTAMP_KEY, VOLUNTEERING_STATE_VARIABLE
from src.pipelines.volunteering_to_typesense.typesense_doc_id import TypesenseDocId
from src.pipelines.volunteering_to_typesense.publish_typesense_changes import publish_volunteering_deletion
from src.pipelines.volunteering_to_typesense.typesense_transformer import transform_row

logger = PipelineLogger(__name__)


@dataclass
class ChangedRecords:
    new_or_updated: List[Dict[str, Any]]
    deleted: List[Dict[str, Any]]


@task
def extract_changed_records(
    table_name: str,
    last_complete_ingestion_date: Optional[date],
    latest_partition_date: date,
) -> ChangedRecords:
    """Extract changed data from BigQuery."""
    logger.info(
        f"Extracting changed records (table_name={table_name}, last_complete_ingestion_date={last_complete_ingestion_date}, latest_partition_date={latest_partition_date})."
    )
    new_or_updated = extract_upserted_records_in_partitions(
        table_name=table_name,
        new_partition_date=latest_partition_date,
        old_partition_date=last_complete_ingestion_date,
    )
    deleted = extract_deleted_records_in_partitions(
        table_name=table_name,
        new_partition_date=latest_partition_date,
        old_partition_date=last_complete_ingestion_date,
    )
    logger.info(f"Extracted {len(new_or_updated)} upserted and {len(deleted)} deleted records")
    return ChangedRecords(
        new_or_updated=new_or_updated,
        deleted=deleted,
    )


def transform_data(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform raw data by processing each row."""
    transformed = [transform_row(row) for row in rows]
    logger.info(f"Transformed {len(rows)} rows of data")
    return transformed


@task
def delete_typesense_documents(
    typesense_client: TypesenseClient,
    typesense_collection_name: str,
    document_ids_to_delete: list[TypesenseDocId],
) -> bool:
    """Deletes documents with the given IDs from the Typesense collection.

    Args:
        typesense_client (TypesenseClient): Typesense client
        typesense_collection_name (str): name of the collection
        document_ids_to_delete (list(TypesenseDocId)): IDs of documents to delete

    Returns:
        bool: True if deletion was successful, False otherwise
    """
    filter_str = f"id: [{', '.join([str(doc_id) for doc_id in document_ids_to_delete])}]"

    try:
        url_encoded_collection_name = quote(typesense_collection_name)
        response = typesense_client.client.collections[url_encoded_collection_name].documents.delete(
            {"filter_by": filter_str}
        )

        num_deleted = response.get("num_deleted")
        deleted_all = num_deleted == len(document_ids_to_delete)

        if deleted_all:
            logger.info(f"Deleted {num_deleted} volunteering documents")
        else:
            missing = len(document_ids_to_delete) - num_deleted
            logger.error(
                f"Partial deletion: {num_deleted}/{len(document_ids_to_delete)} ({missing} missing). IDs: {document_ids_to_delete}"
            )
        return True
    except Exception as e:
        logger.error(
            f"Error deleting {len(document_ids_to_delete)} documents from Typesense: "
            + str(e)
            + "\nFailed document ids: "
            + ",".join([str(doc_id) for doc_id in document_ids_to_delete])
        )
        return False


@task
def validate_collection(typesense_client: TypesenseClient, collection_name: str) -> State:
    """Validate the new collection before switching the alias."""
    # test a sample search
    url_encoded_collection_name = quote(collection_name)
    sample_search = typesense_client.client.collections[url_encoded_collection_name].documents.search(
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


def load_last_complete_ingestion_timestamp() -> datetime | None:
    state = cast(dict[str, Any], variable_value(VOLUNTEERING_STATE_VARIABLE))
    timestamp_str = state.get(LAST_INGESTION_TIMESTAMP_KEY)
    if not timestamp_str:
        return None
    else:
        return datetime.fromisoformat(timestamp_str)


def store_last_complete_ingestion_timestamp(timestamp: datetime) -> None:
    """
    Store the last successful ingestion time in Prefect as a ISO 8601 string.
    """
    state = cast(dict[str, Any], variable_value(VOLUNTEERING_STATE_VARIABLE))
    state[LAST_INGESTION_TIMESTAMP_KEY] = timestamp.isoformat()
    Variable.set(VOLUNTEERING_STATE_VARIABLE, state, overwrite=True)


@task(cache_policy=NONE)
def diff_ingest(
    table_name: str,
    last_complete_ingestion_timestamp: Optional[datetime],
    latest_partition_timestamp: date,
    typesense_client: TypesenseClient,
    pubsub_client: PubSubClient,
    typesense_collection_name: str,
) -> Tuple[bool, bool, int]:
    """
    Incrementally ingests data into a Typesense collection by processing changed records.

    Args:
        table_name (str): the name of the table to extract changed data from
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
        table_name,
        last_complete_ingestion_timestamp.date() if last_complete_ingestion_timestamp is not None else None,
        latest_partition_timestamp,
    )

    processed_items_count = 0

    # Step 2: Transform new or updated records
    new_or_updated_documents = transform_data(changed_records.new_or_updated)

    # Step 3: Delete outdated documents from Typesense
    deletion_succeeded = True
    if changed_records.deleted:
        document_ids_to_delete = [TypesenseDocId(record["id"]) for record in changed_records.deleted]
        deletion_succeeded = delete_typesense_documents(
            typesense_client, typesense_collection_name, document_ids_to_delete
        )
        if deletion_succeeded:
            processed_items_count += len(document_ids_to_delete)

            if not publish_volunteering_deletion(pubsub_client, document_ids_to_delete):
                logger.warning("Failed to publish deletion to Pubsub, but Typesense deletion succeeded")

    # Step 4: Upsert documents into Typesense
    # Please note that deletion must come first. If a record happens to be in both new_or_updated and deleted, this
    # order ensures that the document will still be contained in Typesense after *successful* deletion/upsert of TS.
    upsert_succeeded = upsert_typesense_documents(
        typesense_client, logger, typesense_collection_name, new_or_updated_documents
    )
    if upsert_succeeded:
        processed_items_count += len(new_or_updated_documents)

    return deletion_succeeded, upsert_succeeded, processed_items_count
