from typing import Any, cast, Dict

from prefect import flow, task
from prefect.states import Failed, State, Completed

from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.common.loaders.typesense_loader import upsert_typesense_documents
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.typesense_client import TypesenseClient
from src.common.variables import variable_value, secret_value

logger = PipelineLogger(__name__)

SpaceRow = dict[str, str | None]
SpaceDoc = dict[str, Any | None]


@task
def load_spaces() -> list[SpaceRow]:
    return extract_data_from_bigquery("""
        SELECT
          id,
          name,
          title,
          description,
          location,
          avatar,
          avatar_blurhash,
        FROM
          `public.openbook_communities_community`
        WHERE
          is_deleted=false
    """)


def space_to_doc(space: SpaceRow) -> SpaceDoc:
    return {
        "id": f"space_{str(space.get("id"))}",
        "type": "space",
        "title_de": space.get("title"),
        "title_en": space.get("title"),
        "description_de": space.get("description"),
        "description_en": space.get("description"),
        "location": space.get("location"),
        "location_lat_lng": None,
        "image_url": space.get("avatar"),
        "link_locators": {"space": space.get("id")},
    }


@task
def spaces_to_typesense_docs(spaces: list[SpaceRow]) -> list[SpaceDoc]:
    return [space_to_doc(space) for space in spaces]


@flow
def spaces_to_typesense_ingest_from_scratch() -> State | None:
    config = cast(
        Dict[str, str],
        {
            "api_key": secret_value("typesense-admin-api-key"),
            "host": variable_value("typesense_host"),
            "port": variable_value("typesense_port"),
            "protocol": variable_value("typesense_protocol"),
            "collection": variable_value("typesense_collection"),
        },
    )
    typesense_client = TypesenseClient(config)

    spaces = load_spaces()
    logger.debug("Found %s spaces", len(spaces))

    space_documents = spaces_to_typesense_docs(spaces)
    logger.debug("Transformed %s documents", len(space_documents))

    upsert_succeeded = upsert_typesense_documents(typesense_client, logger, config["collection"], space_documents)
    if not upsert_succeeded:
        return Failed(message="Some documents failed ingestion. Please see error log.")
    else:
        return Completed(message=f"All {len(space_documents)} documents successfully ingested.")
