from datetime import datetime, timezone
from typing import cast

from prefect import State
from prefect.states import Completed, Failed
from typesense.exceptions import ObjectNotFound

from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.common.monitoring.metrics import report_number_of_processed_entities
from src.common.typesense import extract_collection_name_from_schema
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.pipelines.bigquery import volunteering_voltastics_classified_enriched_partitioned, bigquery_okuna_dataset
from src.pipelines.profiles_to_typesense.ingest_from_scratch import profiles_to_typesense_ingest_from_scratch
from src.pipelines.spaces_to_typesense.ingest_from_scratch import spaces_to_typesense_ingest_from_scratch
from src.pipelines.tasks_to_typesense.ingest_from_scratch import (
    tasks_to_typesense_ingest_from_scratch,
)
from src.pipelines.typesense import schema_path
from src.pipelines.volunteering_to_typesense.ingest_updates import volunteering_to_typesense_ingest_updates
from src.pipelines.volunteering_to_typesense.tasks import store_last_complete_ingestion_timestamp

logger = PipelineLogger(__name__)


@monitored_flow
def all_to_typesense_ingest_from_scratch_flow() -> State:
    """
    Orchestrates the ETL pipeline to ingest all search data into Typesense from scratch.
    """
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    source_dataset = str(bigquery_okuna_dataset)
    alias_name = extract_collection_name_from_schema(schema_path=schema_path)

    try:
        active_collection = typesense_client.get_active_collection_name(alias_name)
    except ObjectNotFound:
        # Handle case where alias does not exist
        logger.warning(
            f"Alias '{alias_name}' does not exist while trying to ingest from scratch. Proceeding to create it later."
        )
        active_collection = None

    next_collection = f"{alias_name}_{datetime.now(timezone.utc).isoformat()}"

    typesense_client.drop_collection(next_collection)
    typesense_client.create_collection(schema_file_path=schema_path, collection_name=next_collection)

    logger.info("Ingesting profiles from scratch")
    profiles_result = profiles_to_typesense_ingest_from_scratch(typesense_client, source_dataset, next_collection)
    if isinstance(profiles_result, State) and profiles_result.is_failed():
        return Failed(message=profiles_result.message)

    logger.info("Ingesting spaces from scratch")
    spaces_result = spaces_to_typesense_ingest_from_scratch(typesense_client, source_dataset, next_collection)
    if isinstance(spaces_result, State) and spaces_result.is_failed():
        return Failed(message=spaces_result.message)

    logger.info("Ingesting tasks from scratch")
    tasks_result = tasks_to_typesense_ingest_from_scratch(typesense_client, source_dataset, next_collection)
    if isinstance(tasks_result, State) and tasks_result.is_failed():
        return Failed(message=tasks_result.message)

    logger.info("Ingesting volunteering from scratch")
    volunteering_result = volunteering_to_typesense_ingest_updates(
        typesense_client=typesense_client,
        typesense_collection_name=next_collection,
        table_name=volunteering_voltastics_classified_enriched_partitioned,
        last_complete_ingestion_timestamp=None,
    )
    if isinstance(volunteering_result, State) and volunteering_result.is_failed():
        return Failed(message=volunteering_result.message)

    logger.info(f'Swapping alias {alias_name} to point to newly ingested collection "{next_collection}"')
    typesense_client.set_active_collection(alias_name, next_collection)
    logger.info(f'Alias {alias_name} is now pointing to "{next_collection}".')

    if active_collection is not None:
        logger.info(f'Dropping old collection "{active_collection}"')
        typesense_client.drop_collection(active_collection)

    store_last_complete_ingestion_timestamp(datetime.now(timezone.utc))

    report_number_of_processed_entities(
        cast(int, profiles_result) + cast(int, spaces_result) + cast(int, volunteering_result)
    )

    return Completed()


if __name__ == "__main__":
    all_to_typesense_ingest_from_scratch_flow()
