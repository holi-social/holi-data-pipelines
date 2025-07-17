from prefect import task, flow
from prefect.cache_policies import NONE
from prefect.states import State

from src.common.monitoring.flow import (
    on_cancellation_hooks,
    on_crashed_hooks,
    on_failure_hooks,
    on_completion_hooks,
)
from src.common.pubsub_client import PubSubClient
from src.common.typesense import extract_collection_name_from_schema
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.pipelines.typesense import schema_path
from src.pipelines.volunteering_to_typesense.voltastics_to_typesense_ingest_updates import (
    voltastics_to_typesense_ingest_updates,
)


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def voltastics_to_typesense_ingest_from_scratch_flow() -> State | int:
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    pubsub_client = PubSubClient()
    alias_name = extract_collection_name_from_schema(schema_path=schema_path)
    active_collection = typesense_client.get_active_collection_name(alias_name)

    return voltastics_to_typesense_ingest_updates(
        typesense_client=typesense_client,
        pubsub_client=pubsub_client,
        typesense_collection_name=active_collection,
        last_complete_ingestion_timestamp=None,
    )


@task(cache_policy=NONE)  # type: ignore[arg-type]
def voltastics_to_typesense_ingest_from_scratch(
    typesense_client: TypesenseClient,
    pubsub_client: PubSubClient,
    typesense_collection_name: str,
) -> State | int:
    return voltastics_to_typesense_ingest_updates(
        typesense_client=typesense_client,
        pubsub_client=pubsub_client,
        typesense_collection_name=typesense_collection_name,
        last_complete_ingestion_timestamp=None,
    )


if __name__ == "__main__":
    voltastics_to_typesense_ingest_from_scratch_flow()
