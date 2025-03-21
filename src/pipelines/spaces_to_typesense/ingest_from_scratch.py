from typing import Any, cast, List

from prefect import task
from prefect.cache_policies import NONE
from prefect.states import Failed, State, Completed

from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.common.loaders.typesense_loader import upsert_typesense_documents
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.common.monitoring.metrics import report_number_of_processed_entities
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.common.variables import variable_value

logger = PipelineLogger(__name__)

SpaceRow = dict[str, str | None]
SpaceDoc = dict[str, Any | None]


@task(cache_policy=NONE)  # type: ignore[arg-type]
def load_spaces(source_dataset: str) -> List[SpaceRow]:
    # TODO revert to use this simple query once geolocation_geojson is available in production
    simple_query = f"""
        SELECT
          id,
          name,
          title,
          description,
          location,
          avatar,
          avatar_blurhash,
          geolocation_geojson,
        FROM
          `{source_dataset}.openbook_communities_community`
        WHERE
          is_deleted=false
    """  # noqa: F841, S608
    backwards_compatible_query = f"""
        DECLARE geolocation_geojson_exists BOOL;
        
        -- Check if the geolocation_geojson column exists
        SET geolocation_geojson_exists = (
          SELECT COUNT(*) > 0
          FROM `{source_dataset}.INFORMATION_SCHEMA.COLUMNS`
          WHERE table_name = 'openbook_communities_community'
          AND column_name = 'geolocation_geojson'
        );
        
        -- Execute appropriate SQL based on whether geolocation_geojson exists
        EXECUTE IMMEDIATE FORMAT(\"\"\"
          SELECT
            id,
            name,
            title,
            description,
            location,
            avatar,
            avatar_blurhash,
            %s
          FROM
            `{source_dataset}.openbook_communities_community`
          WHERE
            is_deleted=false
        \"\"\",
          IF(geolocation_geojson_exists, 'geolocation_geojson', 'NULL AS geolocation_geojson')
        );
    """  # noqa: S608
    return extract_data_from_bigquery(backwards_compatible_query)


def to_location_lat_lng(geojson: Any) -> List[float]:
    # GeoJSON uses X (longitude), Y (latitude) order, we need to reverse it to match the lat, lng "standard"
    return [geojson["coordinates"][1], geojson["coordinates"][0]]


def space_to_doc(space: SpaceRow) -> SpaceDoc:
    geojson = space.get("geolocation_geojson")
    return {
        "id": f"space_{str(space.get('id'))}",
        "type": "space",
        "title_de": space.get("title"),
        "title_en": space.get("title"),
        "description_de": space.get("description"),
        "description_en": space.get("description"),
        "location": space.get("location"),
        "location_lat_lng": to_location_lat_lng(geojson) if geojson else None,
        "image_url": space.get("avatar"),
        "link_locators": {"space": space.get("id")},
    }


@task(cache_policy=NONE)  # type: ignore[arg-type]
def spaces_to_typesense_docs(spaces: list[SpaceRow]) -> list[SpaceDoc]:
    return [space_to_doc(space) for space in spaces]


@monitored_flow
def spaces_to_typesense_ingest_from_scratch_flow() -> State:
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    typesense_collection = cast(str, variable_value("typesense_collection"))
    source_dataset = cast(str, variable_value("bigquery_okuna_dataset"))
    task_result = spaces_to_typesense_ingest_from_scratch(typesense_client, source_dataset, typesense_collection)
    if isinstance(task_result, State):
        return task_result
    elif isinstance(task_result, int):
        report_number_of_processed_entities(task_result)
        return Completed()
    else:
        raise ValueError(f"Unexpected task result: {task_result}")


@task(cache_policy=NONE)  # type: ignore[arg-type]
def spaces_to_typesense_ingest_from_scratch(
    typesense_client: TypesenseClient, source_dataset: str, collection: str
) -> State | int:
    logger.info(f"Starting spaces ingestion from scratch into typesense collection '{collection}'")

    spaces = load_spaces(source_dataset)
    logger.info("Found %s spaces", len(spaces))

    space_documents = spaces_to_typesense_docs(spaces)
    logger.info("Transformed %s documents", len(space_documents))

    upsert_succeeded = upsert_typesense_documents(typesense_client, logger, collection, space_documents)
    if not upsert_succeeded:
        return Failed(message="Some documents failed ingestion. Please see error log.")
    else:
        return len(space_documents)


if __name__ == "__main__":
    spaces_to_typesense_ingest_from_scratch_flow()
