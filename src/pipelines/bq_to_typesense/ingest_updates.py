import prefect.logging
from prefect import flow, State
from prefect.states import Failed

from src.common.constants import TYPESENSE_ALIAS_NAME
from src.common.extractors.bq_extractor import (
    extract_latest_partition_timestamp_from_bigquery,
)
from src.common.utils import (
    setup_pipeline,
    load_last_complete_ingestion_timestamp,
    store_last_complete_ingestion_timestamp,
    get_active_collection,
)
from datetime import datetime
from tasks import delete_typesense_documents, upsert_typesense_documents, transform_data, extract_changed_records

# Initialize logging
logger = prefect.logging.get_logger()


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
    if (
        last_complete_ingestion_timestamp
        and last_complete_ingestion_timestamp.date() >= latest_partition_timestamp.date()
    ):
        logger.info(
            f"Last complete ingestion ({last_complete_ingestion_timestamp}) ran on the same day as the newest partition ({latest_partition_timestamp}). Skipping ingestion."
        )
        return

    changed_records = extract_changed_records(
        config.get("volunteering_raw_deduped"),
        last_complete_ingestion_timestamp,
        latest_partition_timestamp,
    )

    new_or_updated_records = [record for record in changed_records if record["change_type"] == "new_or_updated"]
    document_ids_to_delete = [record["id"] for record in changed_records if record["change_type"] == "deleted"]

    new_or_updated_documents = transform_data(new_or_updated_records)

    typesense_collection_name = get_active_collection(typesense_client, TYPESENSE_ALIAS_NAME)

    upsert_succeeded = upsert_typesense_documents(
        typesense_client, typesense_collection_name, new_or_updated_documents
    )

    deletion_succeeded = True
    if document_ids_to_delete:
        deletion_succeeded = delete_typesense_documents(
            typesense_client, typesense_collection_name, document_ids_to_delete
        )

    if upsert_succeeded and deletion_succeeded:
        store_last_complete_ingestion_timestamp(datetime.now())
    else:
        return Failed(message="Some documents failed ingestion. Please see error log.")


if __name__ == "__main__":
    volunteering_to_typesense_etl()
