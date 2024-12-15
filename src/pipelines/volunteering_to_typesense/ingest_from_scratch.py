import prefect.logging
from prefect import flow, State
from prefect.states import Failed, Completed
from typesense.exceptions import ObjectNotFound

from src.common.extractors.bq_extractor import (
    extract_latest_partition_timestamp_from_bigquery,
)
from src.pipelines.volunteering_to_typesense.utils import (
    setup_pipeline,
    store_last_complete_ingestion_timestamp,
    extract_collection_name_from_schema,
    diff_ingest
)
from datetime import datetime
from src.pipelines.volunteering_to_typesense.tasks import validate_collection
from typing import cast

# Initialize logging
logger = prefect.logging.get_logger()


# TODO configure flow via annotation
# TODO configure task so that they can be tried
@flow()
def volunteering_to_typesense_etl() -> None | State:
    """Orchestrates the ETL pipeline to move volunteering data from BigQuery to Typesense."""
    config, typesense_client = setup_pipeline()

    latest_partition_timestamp = extract_latest_partition_timestamp_from_bigquery(
        cast(str, config.get("volunteering_raw_deduped"))
    )

    typesense_alias_name = extract_collection_name_from_schema(cast(str, config["schema_path"]))

    try:
        active_collection = typesense_client.get_active_collection(typesense_alias_name)
    except ObjectNotFound:
        # Handle case where alias does not exist
        logger.warning(f"Alias '{typesense_alias_name}' does not exist while trying to ingest from scratch. Proceeding to create it later.")
        active_collection = ""

    next_collection_name = f"{typesense_alias_name}_{datetime.now().isoformat()}"

    typesense_client.drop_collection(next_collection_name)
    typesense_client.create_collection(cast(str, config["schema_path"]), next_collection_name)

    deletion_succeeded, upsert_succeeded = diff_ingest(config, None,
                                                       latest_partition_timestamp, typesense_client,
                                                       next_collection_name)
    if (
        upsert_succeeded
        and deletion_succeeded
    ):
        validation_result = validate_collection(typesense_client, next_collection_name)
        if validation_result.is_failed():
            return validation_result
        else:
            # Creates/updates an alias
            typesense_client.client.aliases.upsert(typesense_alias_name, {"collection_name": next_collection_name})
            logger.info(f"Alias {typesense_alias_name} now points to {next_collection_name}.")
            store_last_complete_ingestion_timestamp(datetime.now())
            if active_collection:
                typesense_client.drop_collection(active_collection)
            return Completed()
    else:
        return Failed(message="Some documents failed ingestion. Please see error log.")


if __name__ == "__main__":
    volunteering_to_typesense_etl()
