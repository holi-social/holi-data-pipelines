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

TaskRow = dict[str, Any | None]
TaskDoc = dict[str, Any | None]

TOPICS_V2_VECTOR_INDICES = {
    "animal-welfare": 0,
    "arts-culture": 1,
    "climate-environment": 2,
    "democracy-politics": 3,
    "disability-inclusion": 4,
    "disaster-relief": 5,
    "elderly-care": 6,
    "family-kids": 7,
    "food-hunger-relief": 8,
    "gender-equality": 9,
    "housing": 10,
    "human-rights": 11,
    "lgbtqia-plus": 12,
    "lifestyle-consumption": 13,
    "literacy-education": 14,
    "mental-physical-health": 15,
    "migration-refugees": 16,
    "mobility-transport": 17,
    "other": 18,
    "peace-security": 19,
    "poverty-homelessness": 20,
    "sports-recreation": 21,
    "technology-innovation": 22,
    "youth-development": 23,
}

SKILLS_V2_VECTOR_INDICES = {
    "administration": 0,
    "animal-care": 1,
    "arts-crafts": 2,
    "building-handicraft": 3,
    "childcare": 4,
    "community-organizing": 5,
    "creativity-design": 6,
    "driving": 7,
    "emergency-care": 8,
    "event-planning": 9,
    "finances-taxes": 10,
    "food-service": 11,
    "fundraising": 12,
    "gardening-conservation": 13,
    "intercultural-competence": 14,
    "it-technology": 15,
    "legal-assistance": 16,
    "management-strategy": 17,
    "marketing-communications": 18,
    "moderation-facilitation": 19,
    "music-performance": 20,
    "nursing-caregiving": 21,
    "other": 22,
    "religious-spiritual-care": 23,
    "research-analysis": 24,
    "sales-distribution": 25,
    "team-leadership": 26,
    "translation": 27,
    "tutoring-coaching": 28,
}


@task(cache_policy=NONE)  # type: ignore[arg-type]
def load_tasks(source_dataset: str) -> List[TaskRow]:
    query = f"""
        SELECT
          task.*,
          community.topics_v2 as topics_v2
        FROM `{source_dataset}.openbook_communities_task` AS task
        JOIN `{source_dataset}.openbook_communities_community` AS community
          ON task.community_id = community.id
        WHERE community.is_deleted IS FALSE
          AND visibility="PUBLIC"
          AND deleted_at IS NOT NULL
    """  # noqa: S608
    return extract_data_from_bigquery(query)


def to_location_lat_lng(geojson: Any) -> List[float]:
    # GeoJSON uses X (longitude), Y (latitude) order, we need to reverse it to match the lat, lng "standard"
    return [geojson["coordinates"][1], geojson["coordinates"][0]]


def to_topics_skills_embedding_v2(topics_v2: List[str] | None, skills_v2: List[str] | None) -> List[float]:
    # create a zero-filled vector with length equal to the number of skills
    vector_size = len(TOPICS_V2_VECTOR_INDICES) + len(SKILLS_V2_VECTOR_INDICES)
    embedding = [0.0] * vector_size

    # for each topic in the list, set its corresponding position to 1.0
    if topics_v2:
        for topic in topics_v2:
            if topic in TOPICS_V2_VECTOR_INDICES:
                embedding[TOPICS_V2_VECTOR_INDICES[topic]] = 1.0

    # for each skill in the list, set its corresponding position to 1.0
    if skills_v2:
        for skill in skills_v2:
            if skill in SKILLS_V2_VECTOR_INDICES:
                embedding[len(TOPICS_V2_VECTOR_INDICES) + SKILLS_V2_VECTOR_INDICES[skill]] = 1.0

    return embedding


def task_to_doc(row: TaskRow) -> TaskDoc:
    geojson = row.get("geolocation_geojson")
    return {
        "id": f"space_task_{str(row.get('id'))}",
        "type": "space_task",
        "title_de": row.get("name"),
        "title_en": row.get("name"),
        "description_de": row.get("description"),
        "description_en": row.get("description"),
        "location": row.get("location"),
        "location_lat_lng": to_location_lat_lng(geojson) if geojson else None,
        "image_url": row.get("thumbnail"),
        "link_locators": {"space": row.get("community_id"), "task": row.get("id")},
        "topics_v2": row.get("topics_v2"),
        "skills_v2": row.get("skills_v2"),
        "topics_skills_embedding_v2": to_topics_skills_embedding_v2(
            cast(List[str], row.get("topics_v2")), cast(List[str], row.get("skills_v2"))
        ),
        "similarity_embedding": None,
    }


@task(cache_policy=NONE)  # type: ignore[arg-type]
def tasks_to_typesense_docs(tasks: list[TaskRow]) -> list[TaskDoc]:
    return [task_to_doc(task) for task in tasks]


@monitored_flow
def tasks_to_typesense_ingest_from_scratch_flow() -> State:
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    typesense_collection = cast(str, variable_value("typesense_collection"))
    source_dataset = cast(str, variable_value("bigquery_okuna_dataset"))
    task_result = tasks_to_typesense_ingest_from_scratch(typesense_client, source_dataset, typesense_collection)
    if isinstance(task_result, State):
        return task_result
    elif isinstance(task_result, int):
        report_number_of_processed_entities(task_result)
        return Completed()
    else:
        raise ValueError(f"Unexpected task result: {task_result}")


@task(cache_policy=NONE)  # type: ignore[arg-type]
def tasks_to_typesense_ingest_from_scratch(
    typesense_client: TypesenseClient, source_dataset: str, collection: str
) -> State | int:
    logger.info(f"Starting tasks ingestion from scratch into typesense collection '{collection}'")

    tasks = load_tasks(source_dataset)
    logger.info("Found %s tasks", len(tasks))

    task_documents = tasks_to_typesense_docs(tasks)
    logger.info("Transformed %s documents", len(task_documents))
    logger.info(task_documents)

    upsert_succeeded = upsert_typesense_documents(typesense_client, logger, collection, task_documents)
    if not upsert_succeeded:
        return Failed(message="Some documents failed ingestion. Please see error log.")
    else:
        return len(task_documents)


if __name__ == "__main__":
    tasks_to_typesense_ingest_from_scratch_flow()
