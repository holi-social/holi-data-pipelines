import prefect.logging
from prefect import flow, State
from prefect.states import Failed

from src.common.constants import TYPESENSE_ALIAS_NAME
from src.common.extractors.bq_extractor import (
    extract_latest_partition_timestamp_from_bigquery,
)
from src.pipelines.bq_to_typesense.utils import (
    setup_pipeline,
    store_last_complete_ingestion_timestamp,
    extract_collection_name_from_schema,
    diff_ingest,
    get_active_collection
)
from datetime import datetime
from tasks import validate_collection

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

    current_collection = get_active_collection(typesense_client, TYPESENSE_ALIAS_NAME)
    next_collection = f"{typesense_collection_name}_{datetime.now().isoformat()}"

    typesense_client.drop_collection(next_collection)
    typesense_client.create_collection(config["schema_path"], next_collection)

    deletion_succeeded, upsert_succeeded = diff_ingest(config, None,
                                                       latest_partition_timestamp, typesense_client,
                                                       next_collection)

    # TODO: add another return value chnaged_records
    # Switch the alias to the next collection if the ingestion succeeded
    if (
        upsert_succeeded
        and deletion_succeeded
    ):
        validation_result = validate_collection(typesense_client, next_collection)
        if validation_result.is_failed():
            return validation_result
        else:
            typesense_client.aliases[TYPESENSE_ALIAS_NAME].upsert({"collection_name": next_collection})
            logger.info(f"Alias {TYPESENSE_ALIAS_NAME} now points to {next_collection}.")
            store_last_complete_ingestion_timestamp(datetime.now())
            typesense_client.drop_collection(current_collection)
    else:
        return Failed(message="Some documents failed ingestion. Please see error log.")


if __name__ == "__main__":
    volunteering_to_typesense_etl()
