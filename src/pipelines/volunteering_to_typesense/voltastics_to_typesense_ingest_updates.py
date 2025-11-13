from datetime import datetime, timezone

from prefect import task, State, flow
from prefect.cache_policies import NONE
from prefect.states import Failed, Completed

from src.common.extractors.bq_extractor import (
    extract_latest_partition_timestamp_from_bigquery,
)
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import (
    on_cancellation_hooks,
    on_crashed_hooks,
    on_failure_hooks,
    on_completion_hooks,
)
from src.common.monitoring.metrics import report_number_of_processed_entities
from src.common.external_data_pubsub_client import ExternalDataPubSubClient
from src.common.typesense import extract_collection_name_from_schema
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.pipelines.bigquery import volunteering_voltastics_classified_enriched_partitioned
from src.pipelines.typesense import schema_path, validate_collection
from src.pipelines.volunteering_to_typesense.tasks import (
    diff_ingest,
    store_last_complete_ingestion_timestamp,
    load_last_complete_ingestion_timestamp,
)

logger = PipelineLogger(__name__)


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def voltastics_to_typesense_ingest_updates_flow() -> State:
    """
    Orchestrates the ETL pipeline to move volunteering data from BigQuery to Typesense.
    """
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    pubsub_client = ExternalDataPubSubClient()
    alias_name = extract_collection_name_from_schema(schema_path=schema_path())
    active_collection = typesense_client.get_active_collection_name(alias_name)
    task_result = voltastics_to_typesense_ingest_updates(
        typesense_client=typesense_client,
        pubsub_client=pubsub_client,
        typesense_collection_name=active_collection,
        last_complete_ingestion_timestamp=load_last_complete_ingestion_timestamp(),
    )
    if isinstance(task_result, State):
        return task_result
    elif isinstance(task_result, int):
        store_last_complete_ingestion_timestamp(datetime.now(timezone.utc))
        report_number_of_processed_entities(task_result)
        return Completed()
    else:
        return Failed(message=f"Unexpected task result: {task_result}")


@task(cache_policy=NONE)  # type: ignore[arg-type]
def voltastics_to_typesense_ingest_updates(
    typesense_client: TypesenseClient,
    pubsub_client: ExternalDataPubSubClient,
    typesense_collection_name: str,
    last_complete_ingestion_timestamp: datetime | None,
) -> State | int:
    table_name = volunteering_voltastics_classified_enriched_partitioned()
    latest_partition_timestamp = extract_latest_partition_timestamp_from_bigquery(table_name)

    # volunteering data is partitioned by day so we compare days only
    if (
        last_complete_ingestion_timestamp
        and last_complete_ingestion_timestamp.date() >= latest_partition_timestamp.date()
    ):
        logger.info(
            f"Last complete ingestion ({last_complete_ingestion_timestamp}) ran on the same day as the newest partition ({latest_partition_timestamp}). Skipping ingestion."
        )
        return 0

    deletion_succeeded, upsert_succeeded, processed_item_count = diff_ingest(
        table_name=table_name,
        last_complete_ingestion_timestamp=last_complete_ingestion_timestamp,
        latest_partition_timestamp=latest_partition_timestamp,
        typesense_client=typesense_client,
        pubsub_client=pubsub_client,
        typesense_collection_name=typesense_collection_name,
    )

    if upsert_succeeded and deletion_succeeded:
        validation_result = validate_collection(typesense_client, typesense_collection_name)
        if validation_result.is_failed():
            return validation_result
        else:
            return processed_item_count
    else:
        return Failed(message="Some volunteering documents failed ingestion. Please see error log.")


if __name__ == "__main__":
    voltastics_to_typesense_ingest_updates_flow()
