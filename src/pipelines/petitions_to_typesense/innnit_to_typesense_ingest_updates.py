from datetime import datetime, timezone
from typing import Any

from prefect import State, task, flow
from prefect.cache_policies import NONE
from prefect.states import Failed, Completed

from src.common.extractors.bq_extractor import extract_latest_partition_timestamp_from_bigquery
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import on_cancellation_hooks, on_completion_hooks, on_crashed_hooks, on_failure_hooks
from src.common.monitoring.metrics import report_number_of_processed_entities
from src.common.typesense import extract_collection_name_from_schema
from src.pipelines.bigquery import petitions_innnit_raw
from src.common.external_data_pubsub_client import ExternalDataPubSubClient
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.common.variables import update_variable, variable_value_as
from src.pipelines.constants import INNNIT_STATE_VARIABLE, LAST_INGESTION_TIMESTAMP_KEY
from src.pipelines.petitions_to_typesense.diff_ingest import diff_ingest
from src.pipelines.typesense import schema_path, validate_collection

logger = PipelineLogger(__name__)


class _InnnitIngestionTimestamp:
    @staticmethod
    def get() -> datetime | None:
        state = variable_value_as(dict[str, Any], INNNIT_STATE_VARIABLE, default={})
        timestamp_str = state.get(LAST_INGESTION_TIMESTAMP_KEY)

        return datetime.fromisoformat(timestamp_str) if timestamp_str else None

    @staticmethod
    def set(timestamp: datetime) -> None:
        state = variable_value_as(dict[str, Any], INNNIT_STATE_VARIABLE, default={})
        state[LAST_INGESTION_TIMESTAMP_KEY] = timestamp.isoformat()
        update_variable(INNNIT_STATE_VARIABLE, state)


@task(cache_policy=NONE)
def petitions_innnit_to_typesense_ingest_updates(
    typesense: TypesenseClient,
    pubsub: ExternalDataPubSubClient,
    typesense_collection: str,
    last_complete_ingestion_timestamp: datetime | None,
) -> State | int:
    table_name = petitions_innnit_raw()
    latest_partition_timestamp = extract_latest_partition_timestamp_from_bigquery(
        table_name, partition_time_column_name="_PARTITIONTIME"
    )

    if (
        last_complete_ingestion_timestamp
        and last_complete_ingestion_timestamp.date() >= latest_partition_timestamp.date()
    ):
        logger.info(
            f"Last complete ingestion ({last_complete_ingestion_timestamp}) ran on the same day or later than newest partition ({latest_partition_timestamp}). Skipping."
        )
        return 0

    deletion_succeeded, upsert_succeeded, processed_item_count = diff_ingest(
        table_name=table_name,
        last_complete_ingestion_timestamp=last_complete_ingestion_timestamp,
        latest_partition_timestamp=latest_partition_timestamp,
        typesense=typesense,
        pubsub=pubsub,
        typesense_collection=typesense_collection,
    )

    if upsert_succeeded and deletion_succeeded:
        validation_result = validate_collection(typesense, typesense_collection)
        if validation_result.is_failed():
            return Failed(message=f"Collection validation failed after innn.it ingestion: {validation_result.message}")
        return processed_item_count
    else:
        return Failed(message="Some innn.it documents failed ingestion")


@flow(
    on_completion=on_completion_hooks,
    on_failure=on_failure_hooks,
    on_crashed=on_crashed_hooks,
    on_cancellation=on_cancellation_hooks,
)
def petitions_innnit_to_typesense_ingest_updates_flow() -> State:
    typesense = TypesenseClient(TypesenseClientConfig.from_env())
    pubsub = ExternalDataPubSubClient()
    alias_name = extract_collection_name_from_schema(schema_path=schema_path())
    active_collection = typesense.get_active_collection_name(alias_name)

    task_result = petitions_innnit_to_typesense_ingest_updates(
        typesense=typesense,
        pubsub=pubsub,
        typesense_collection=active_collection,
        last_complete_ingestion_timestamp=_InnnitIngestionTimestamp.get(),
    )

    if isinstance(task_result, int):
        _InnnitIngestionTimestamp.set(datetime.now(timezone.utc))
        report_number_of_processed_entities(task_result)
        logger.info(f"Successfully processed {task_result} innn.it records")
        return Completed()

    return Failed()
