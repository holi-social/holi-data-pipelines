from datetime import datetime
from typing import cast, Any

from prefect import flow, State
from prefect.states import Failed, Completed
from prefect.futures import PrefectFuture
from typesense.exceptions import ObjectNotFound

from src.common.logging.PipelineLogger import PipelineLogger
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.pipelines.volunteering_to_typesense.ingest_updates import volunteering_to_typesense_ingest_updates
from src.common.typesense import extract_collection_name_from_schema, get_config
from src.pipelines.volunteering_to_typesense.tasks import store_last_complete_ingestion_timestamp
from src.pipelines.profiles_to_typesense.ingest_from_scratch import profiles_to_typesense_ingest_from_scratch
from src.pipelines.spaces_to_typesense.ingest_from_scratch import spaces_to_typesense_ingest_from_scratch

logger = PipelineLogger(__name__)


@flow()
def all_to_typesense_ingest_from_scratch_flow() -> State:
    """
    Orchestrates the ETL pipeline to ingest all search data into Typesense from scratch.
    """
    config = get_config()
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    alias_name = extract_collection_name_from_schema(cast(str, config["schema_path"]))

    try:
        active_collection = typesense_client.get_active_collection_name(alias_name)
    except ObjectNotFound:
        # Handle case where alias does not exist
        logger.warning(
            f"Alias '{alias_name}' does not exist while trying to ingest from scratch. Proceeding to create it later."
        )
        active_collection = None

    next_collection = f"{alias_name}_{datetime.now().isoformat()}"

    typesense_client.drop_collection(next_collection)
    typesense_client.create_collection(
        schema_file_path=cast(str, config["schema_path"]), collection_name=next_collection
    )

    profiles_future: PrefectFuture[Any] = profiles_to_typesense_ingest_from_scratch.submit(
        typesense_client, next_collection
    )
    spaces_future: PrefectFuture[Any] = spaces_to_typesense_ingest_from_scratch.submit(
        typesense_client, next_collection
    )
    volunteering_future: PrefectFuture[Any] = volunteering_to_typesense_ingest_updates.submit(
        typesense_client=typesense_client,
        typesense_collection_name=next_collection,
        table_name=cast(str, config.get("volunteering_raw_deduped")),
        last_complete_ingestion_timestamp=None,
    )

    logger.info("Submitted tasks for parallel execution (profiles, spaces, volunteering). Waiting for results...")

    profiles_result = profiles_future.result(raise_on_failure=False)
    spaces_result = spaces_future.result(raise_on_failure=False)
    volunteering_result = volunteering_future.result(raise_on_failure=False)

    if (
        not profiles_future.state.is_completed()
        or not spaces_future.state.is_completed()
        or not volunteering_future.state.is_completed()
    ):
        logger.error(
            "One or multiple of the ingestion tasks did not complete successfully. %s",
            {
                "profiles": str(profiles_result),
                "spaces": str(spaces_result),
                "volunteering": str(volunteering_result),
            },
        )
        return Failed(message="One of the ingestion tasks did not complete successfully. Check logs for details.")

    typesense_client.set_active_collection(alias_name, next_collection)
    logger.info(f"{next_collection} is now active under the alias {alias_name}.")
    if active_collection is not None:
        typesense_client.drop_collection(active_collection)
    store_last_complete_ingestion_timestamp(datetime.now())
    return Completed()


if __name__ == "__main__":
    all_to_typesense_ingest_from_scratch_flow()
