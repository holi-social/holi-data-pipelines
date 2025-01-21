from datetime import datetime
from typing import cast

from prefect import task, State
from prefect.cache_policies import NONE
from prefect.states import Failed, Completed

from src.common.extractors.bq_extractor import (
    extract_latest_partition_timestamp_from_bigquery,
)
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.alerting import alerting_flow
from src.common.typesense import extract_collection_name_from_schema, get_config
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.pipelines.volunteering_to_typesense.tasks import (
    validate_collection,
    diff_ingest,
    store_last_complete_ingestion_timestamp,
    load_last_complete_ingestion_timestamp,
)

logger = PipelineLogger(__name__)


@alerting_flow
def volunteering_to_typesense_ingest_updates_flow() -> State:
    """
    Orchestrates the ETL pipeline to move volunteering data from BigQuery to Typesense.
    """
    config = get_config()
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())

    alias_name = extract_collection_name_from_schema(cast(str, config["schema_path"]))

    active_collection = typesense_client.get_active_collection_name(alias_name)

    result = volunteering_to_typesense_ingest_updates(
        typesense_client=typesense_client,
        typesense_collection_name=active_collection,
        table_name=cast(str, config.get("volunteering_raw_deduped")),
        last_complete_ingestion_timestamp=load_last_complete_ingestion_timestamp(),
    )

    if result.is_completed():
        store_last_complete_ingestion_timestamp(datetime.now())

    return result


@task(cache_policy=NONE)  # type: ignore[arg-type]
def volunteering_to_typesense_ingest_updates(
    typesense_client: TypesenseClient,
    typesense_collection_name: str,
    table_name: str,
    last_complete_ingestion_timestamp: datetime | None,
) -> State:
    latest_partition_timestamp = extract_latest_partition_timestamp_from_bigquery(table_name)

    # volunteering data is partitioned by day so we compare days only
    if (
        last_complete_ingestion_timestamp
        and last_complete_ingestion_timestamp.date() >= latest_partition_timestamp.date()
    ):
        logger.info(
            f"Last complete ingestion ({last_complete_ingestion_timestamp}) ran on the same day as the newest partition ({latest_partition_timestamp}). Skipping ingestion."
        )
        return Completed()

    deletion_succeeded, upsert_succeeded = diff_ingest(
        table_name=table_name,
        last_complete_ingestion_timestamp=last_complete_ingestion_timestamp,
        latest_partition_timestamp=latest_partition_timestamp,
        typesense_client=typesense_client,
        typesense_collection_name=typesense_collection_name,
    )

    if upsert_succeeded and deletion_succeeded:
        validation_result = validate_collection(typesense_client, typesense_collection_name)
        if validation_result.is_failed():
            return validation_result
        else:
            return Completed()
    else:
        return Failed(message="Some volunteering documents failed ingestion. Please see error log.")


if __name__ == "__main__":
    volunteering_to_typesense_ingest_updates_flow()
