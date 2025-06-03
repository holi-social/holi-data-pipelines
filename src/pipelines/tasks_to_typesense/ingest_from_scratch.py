from typing import Any, List, cast

from prefect import task
from prefect.cache_policies import NONE
from prefect.states import Completed, Failed, State

from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.common.loaders.typesense_loader import upsert_typesense_documents
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.common.monitoring.metrics import report_number_of_processed_entities
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.common.variables import variable_value
from src.pipelines.bigquery import tasks_with_classifications

logger = PipelineLogger(__name__)

TaskRow = dict[str, Any | None]
TaskDoc = dict[str, Any | None]


@task(cache_policy=NONE)  # type: ignore[arg-type]
def load_tasks() -> List[TaskRow]:
    return extract_data_from_bigquery(f"SELECT * FROM {tasks_with_classifications()}")  # noqa: S608


def to_location_lat_lng(geojson: Any) -> List[float]:
    # GeoJSON uses X (longitude), Y (latitude) order, we need to reverse it to match the lat, lng "standard"
    return [geojson["coordinates"][1], geojson["coordinates"][0]]


def task_to_doc(row: TaskRow) -> TaskDoc:
    geojson = row.get("geolocation_geojson")
    return {
        "id": f"task_{str(row.get('id'))}",
        "space_id": str(row.get("community_id")),
        "type": "task",
        "title_de": row.get("name"),
        "title_en": row.get("name"),
        "description_de": row.get("description"),
        "description_en": row.get("description"),
        "location": row.get("location"),
        "location_lat_lng": to_location_lat_lng(geojson) if geojson else None,
        "location_type": row.get("location_type"),
        "commitment_label": row.get("regularity"),
        "commitment_duration": row.get("duration"),
        "image_url": row.get("thumbnail"),
        "organizer_name": row.get("community_name"),
        "organizer_image_url": row.get("community_avatar"),
        "link_locators": {"space": row.get("community_slug"), "task": row.get("id")},
        "topics_v2": row.get("topics_v2"),
        "skills_v2": row.get("skills_v2"),
        "topics_skills_embedding_v2": row.get("topics_skills_embedding_v2"),
        "similarity_embedding": row.get("similarity_embedding"),
    }


@task(cache_policy=NONE)  # type: ignore[arg-type]
def tasks_to_typesense_docs(tasks: list[TaskRow]) -> list[TaskDoc]:
    return [task_to_doc(task) for task in tasks]


@monitored_flow
def tasks_to_typesense_ingest_from_scratch_flow() -> State:
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    typesense_collection = cast(str, variable_value("typesense_collection"))
    task_result = tasks_to_typesense_ingest_from_scratch(typesense_client, typesense_collection)
    if isinstance(task_result, State):
        return task_result
    elif isinstance(task_result, int):
        report_number_of_processed_entities(task_result)
        return Completed()
    else:
        raise ValueError(f"Unexpected task result: {task_result}")


@task(cache_policy=NONE)  # type: ignore[arg-type]
def tasks_to_typesense_ingest_from_scratch(typesense_client: TypesenseClient, collection: str) -> State | int:
    logger.info(f"Starting tasks ingestion from scratch into typesense collection '{collection}'")

    tasks = load_tasks()
    logger.info("Found %s tasks", len(tasks))

    task_documents = tasks_to_typesense_docs(tasks)
    logger.info("Transformed %s documents", len(task_documents))

    upsert_succeeded = upsert_typesense_documents(typesense_client, logger, collection, task_documents)
    if not upsert_succeeded:
        return Failed(message="Some documents failed ingestion. Please see error log.")
    else:
        return len(task_documents)


if __name__ == "__main__":
    tasks_to_typesense_ingest_from_scratch_flow()
