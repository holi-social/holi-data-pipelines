import prefect.logging
from prefect import flow, State
from prefect.states import Failed

from src.common.constants import TYPESENSE_ALIAS_NAME
from src.common.extractors.bq_extractor import (
    extract_latest_partition_timestamp_from_bigquery,
)
from src.pipelines.volunteering_to_typesense.utils import (
    setup_pipeline,
    load_last_complete_ingestion_timestamp,
    store_last_complete_ingestion_timestamp,
    diff_ingest
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

    typesense_collection_name = typesense_client.get_active_collection(TYPESENSE_ALIAS_NAME)

    deletion_succeeded, upsert_succeeded = diff_ingest(config, last_complete_ingestion_timestamp,
                                                       latest_partition_timestamp, typesense_client,
                                                       typesense_collection_name)

    if upsert_succeeded and deletion_succeeded:
        validation_result = validate_collection(typesense_client, typesense_collection_name)
        if validation_result.is_failed():
            return validation_result
        else:
            store_last_complete_ingestion_timestamp(datetime.now())
    else:
        return Failed(message="Some documents failed ingestion. Please see error log.")


if __name__ == "__main__":
    volunteering_to_typesense_etl()
