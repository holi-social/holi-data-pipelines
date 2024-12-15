from datetime import datetime
from typing import cast

import prefect.logging
from prefect import flow, State
from prefect.states import Failed, Completed
from typesense.exceptions import ObjectNotFound

from src.common.extractors.bq_extractor import (
    extract_latest_partition_timestamp_from_bigquery,
)
from src.pipelines.volunteering_to_typesense.tasks import (
    validate_collection,
    setup_pipeline,
    store_last_complete_ingestion_timestamp,
    extract_collection_name_from_schema,
    diff_ingest
)

# Initialize logging
logger = prefect.logging.get_logger()


# TODO configure flow via annotation
# TODO configure task so that they can be tried
@flow()
def volunteering_to_typesense_ingest_from_scratch() -> None | State:
    """Orchestrates the ETL pipeline to move volunteering data from BigQuery to Typesense."""
    config, typesense_client = setup_pipeline()

    latest_partition_timestamp = extract_latest_partition_timestamp_from_bigquery(
        cast(str, config.get("volunteering_raw_deduped"))
    )

    alias_name = extract_collection_name_from_schema(cast(str, config["schema_path"]))

    try:
        active_collection = typesense_client.get_active_collection(alias_name)
    except ObjectNotFound:
        # Handle case where alias does not exist
        logger.warning(f"Alias '{alias_name}' does not exist while trying to ingest from scratch. Proceeding to create it later.")
        active_collection = None

    next_collection = f"{alias_name}_{datetime.now().isoformat()}"

    typesense_client.drop_collection(next_collection)
    typesense_client.create_collection(schema_file_path=cast(str, config["schema_path"]), collection_name=next_collection)

    deletion_succeeded, upsert_succeeded = diff_ingest(config=config,
                                                       last_complete_ingestion_timestamp=None,
                                                       latest_partition_timestamp=latest_partition_timestamp,
                                                       typesense_client=typesense_client,
                                                       typesense_collection_name=next_collection)
    if upsert_succeeded and deletion_succeeded:
        validation_result = validate_collection(typesense_client, next_collection)
        if validation_result.is_failed():
            return validation_result
        else:
            typesense_client.set_active_collection(alias_name, next_collection)
            logger.info(f"{next_collection} is now active under the alias {alias_name}.")
            store_last_complete_ingestion_timestamp(datetime.now())
            if active_collection is not None:
                typesense_client.drop_collection(active_collection)
            return Completed()
    else:
        return Failed(message="Some documents failed ingestion. Please see error log.")


if __name__ == "__main__":
    volunteering_to_typesense_ingest_from_scratch()
