from datetime import datetime, timezone

from prefect import State, task
from prefect.cache_policies import NONE
from prefect.states import Failed
from prefect.variables import Variable
from typing import Dict, Any, cast

from src.common.extractors.bq_extractor import extract_latest_partition_timestamp_from_bigquery
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.common.monitoring.metrics import report_number_of_processed_entities
from src.common.typesense import extract_collection_name_from_schema
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.pipelines.bigquery import volunteering_aktion_mensch_classified_enriched_partitioned
from src.pipelines.typesense import schema_path, LAST_INGESTION_TIMESTAMP_KEY, AKTION_MENSCH_STATE_VARIABLE
from src.pipelines.volunteering_to_typesense.tasks import (
    validate_collection,
    diff_ingest,
)

logger = PipelineLogger(__name__)


def load_am_ingestion_timestamp() -> datetime | None:
    """Load the timestamp of the last successful ingestion from Prefect variables."""
    state = cast(Dict[str, Any], Variable.get(AKTION_MENSCH_STATE_VARIABLE, default={}))
    timestamp_str = state.get(LAST_INGESTION_TIMESTAMP_KEY)
    if not timestamp_str:
        return None
    else:
        return datetime.fromisoformat(timestamp_str)


def store_am_ingestion_timestamp(timestamp: datetime) -> None:
    """Store the timestamp of the last successful ingestion in Prefect variables."""
    state = cast(Dict[str, Any], Variable.get(AKTION_MENSCH_STATE_VARIABLE, default={}))
    state[LAST_INGESTION_TIMESTAMP_KEY] = timestamp.isoformat()
    Variable.set(AKTION_MENSCH_STATE_VARIABLE, state, overwrite=True)


@monitored_flow
def aktion_mensch_to_typesense_ingest_updates_flow() -> None:
    """Flow to ingest Aktion Mensch volunteering opportunities into Typesense."""
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    alias_name = extract_collection_name_from_schema(schema_path=schema_path)
    active_collection = typesense_client.get_active_collection_name(alias_name)

    task_result = aktion_mensch_to_typesense_ingest_updates(
        typesense_client=typesense_client,
        typesense_collection_name=active_collection,
        last_complete_ingestion_timestamp=load_am_ingestion_timestamp(),
    )

    if isinstance(task_result, int):
        store_am_ingestion_timestamp(datetime.now(timezone.utc))
        report_number_of_processed_entities(task_result)
        logger.info(f"Successfully processed {task_result} Aktion Mensch records")


@task(cache_policy=NONE)  # type: ignore[arg-type]
def aktion_mensch_to_typesense_ingest_updates(
    typesense_client: TypesenseClient,
    typesense_collection_name: str,
    last_complete_ingestion_timestamp: datetime | None,
) -> State | int:
    """Task to extract, transform and load AM data to Typesense."""
    table_name = volunteering_aktion_mensch_classified_enriched_partitioned()
    latest_partition_timestamp = extract_latest_partition_timestamp_from_bigquery(table_name)

    # Skip if no new data since last run
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
        typesense_client=typesense_client,
        typesense_collection_name=typesense_collection_name,
    )

    if upsert_succeeded and deletion_succeeded:
        validation_result = validate_collection(typesense_client, typesense_collection_name)
        if validation_result.is_failed():
            return Failed(message="Collection validation failed after Aktion Mensch ingestion")
        return processed_item_count
    else:
        return Failed(message="Some Aktion Mensch documents failed ingestion")


if __name__ == "__main__":
    aktion_mensch_to_typesense_ingest_updates_flow()
