from prefect import task
from prefect.cache_policies import NONE
from prefect.states import State

from src.common.monitoring.flow import monitored_flow
from src.common.typesense import extract_collection_name_from_schema
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.pipelines.typesense import schema_path
from src.pipelines.aktion_mensch_to_typesense.ingest_updates import aktion_mensch_to_typesense_ingest_updates


@monitored_flow
def aktion_mensch_to_typesense_ingest_from_scratch_flow() -> State | int:
    """Ingest Aktion Mensch volunteering data into Typesense from scratch."""
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    alias_name = extract_collection_name_from_schema(schema_path=schema_path)
    active_collection = typesense_client.get_active_collection_name(alias_name)
    return aktion_mensch_to_typesense_ingest_from_scratch(
        typesense_client=typesense_client,
        typesense_collection_name=active_collection,
    )


@task(cache_policy=NONE)  # type: ignore[arg-type]
def aktion_mensch_to_typesense_ingest_from_scratch(
    typesense_client: TypesenseClient,
    typesense_collection_name: str,
) -> State | int:
    """Task to ingest Aktion Mensch data into Typesense from scratch."""
    return aktion_mensch_to_typesense_ingest_updates(
        typesense_client=typesense_client,
        typesense_collection_name=typesense_collection_name,
        last_complete_ingestion_timestamp=None,
    )


if __name__ == "__main__":
    aktion_mensch_to_typesense_ingest_from_scratch_flow()
