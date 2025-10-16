from prefect import State, task, flow
from prefect.cache_policies import NONE

from src.common.external_data_pubsub_client import ExternalDataPubSubClient
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import on_cancellation_hooks, on_completion_hooks, on_crashed_hooks, on_failure_hooks
from src.common.typesense import extract_collection_name_from_schema
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.pipelines.petitions_to_typesense.innnit_to_typesense_ingest_updates import (
    petitions_innnit_to_typesense_ingest_updates,
)
from src.pipelines.typesense import schema_path

logger = PipelineLogger(__name__)


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def petitions_innnit_to_typesense_ingest_from_scratch_flow() -> State | int:
    typesense = TypesenseClient(TypesenseClientConfig.from_env())
    pubsub = ExternalDataPubSubClient()

    typesense_alias_name = extract_collection_name_from_schema(schema_path=schema_path())
    active_typesense_collection = typesense.get_active_collection_name(typesense_alias_name)

    return petitions_innnit_to_typesense_ingest_from_scratch(
        typesense,
        pubsub,
        active_typesense_collection,
    )


@task(cache_policy=NONE)
def petitions_innnit_to_typesense_ingest_from_scratch(
    typesense: TypesenseClient,
    pubsub: ExternalDataPubSubClient,
    typesense_collection: str,
) -> State | int:
    return petitions_innnit_to_typesense_ingest_updates(
        typesense,
        pubsub,
        typesense_collection,
        last_complete_ingestion_timestamp=None,
    )


if __name__ == "__main__":
    petitions_innnit_to_typesense_ingest_from_scratch_flow()
