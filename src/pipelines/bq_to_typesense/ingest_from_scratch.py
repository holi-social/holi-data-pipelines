import prefect.logging
from prefect import flow, State
from prefect.states import Failed

from src.common.constants import TYPESENSE_ALIAS_NAME
from src.common.extractors.bq_extractor import (
    extract_latest_partition_timestamp_from_bigquery,
)
from src.common.utils import (
    setup_pipeline,
    store_last_complete_ingestion_timestamp,
    extract_collection_name_from_schema,
    validate_collection,
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

    latest_partition_timestamp = extract_latest_partition_timestamp_from_bigquery(
        config.get("volunteering_raw_deduped")
    )

    typesense_collection_name = extract_collection_name_from_schema(config["schema_path"])

    blue_collection = f"{typesense_collection_name}_blue"
    green_collection = f"{typesense_collection_name}_green"

    current_collection = (
        green_collection
        if typesense_client.aliases[TYPESENSE_ALIAS_NAME].retrieve()["collection_name"] == green_collection
        else blue_collection
    )

    next_collection = green_collection if current_collection == blue_collection else blue_collection

    typesense_client.drop_collection(next_collection)
    typesense_client.create_collection(config["schema_path"], next_collection)

    changed_records = extract_changed_records(
        config.get("volunteering_raw_deduped"),
        None,
        latest_partition_timestamp,
    )

    new_or_updated_records = [record for record in changed_records if record["change_type"] == "new_or_updated"]
    document_ids_to_delete = [record["id"] for record in changed_records if record["change_type"] == "deleted"]

    new_or_updated_documents = transform_data(new_or_updated_records)

    upsert_succeeded = upsert_typesense_documents(typesense_client, next_collection, new_or_updated_documents)

    deletion_succeeded = True
    if document_ids_to_delete:
        deletion_succeeded = delete_typesense_documents(typesense_client, next_collection, document_ids_to_delete)
    # Switch the alias to the next collection if the ingestion succeeded
    if (
        upsert_succeeded
        and deletion_succeeded
        and validate_collection(typesense_client, next_collection, len(changed_records))
    ):
        typesense_client.aliases[TYPESENSE_ALIAS_NAME].upsert({"collection_name": next_collection})
        logger.info(f"Alias {TYPESENSE_ALIAS_NAME} now points to {next_collection}.")
        store_last_complete_ingestion_timestamp(datetime.now())
        typesense_client.drop_collection(current_collection)
    else:
        return Failed(message="Some documents failed ingestion. Please see error log.")


if __name__ == "__main__":
    volunteering_to_typesense_etl()
