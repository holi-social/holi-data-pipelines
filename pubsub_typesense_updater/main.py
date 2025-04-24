import os
import dataclasses
import logging
from typing import Union, Optional, List, Any

import vertexai
import functions_framework
from google.cloud import bigquery, logging as gcloud_logging
from cloudevents.http.event import CloudEvent
from events import (
    SpaceCreatedEvent,
    SpaceDeletedEvent,
    SpaceUpdatedEvent,
    TaskCreatedEvent,
    TaskDeletedEvent,
    TaskUpdatedEvent,
    UserDeletedEvent,
    UserUpdatedEvent,
    get_event_type,
)
from task_classifier import TaskClassifier
from shared.classifiers.classification_item import ClassificationItem

from typesense_client import TypesenseClient
from typesense.exceptions import ObjectNotFound

# don't log to Google Cloud Logging if we're run in a test
if os.environ.get("PYTEST_CURRENT_TEST") is not None:
    gcloud_logging_client = gcloud_logging.Client()  # type: ignore
    gcloud_logging_client.setup_logging(log_level=logging.DEBUG)  # type: ignore

logger = logging.getLogger(__name__)


def gcloud_project_id() -> str:
    return os.environ.get("GCLOUD_PROJECT_ID", "")


def gcloud_location() -> str:
    return os.environ.get("GCLOUD_LOCATION", "")


def bigquery_table_name_classifications() -> str:
    return os.environ.get("BIGQUERY_TABLE_NAME_CLASSIFICATIONS", "")


def process_user_updated_event(ts_client: TypesenseClient, event: UserUpdatedEvent) -> None:
    logger.debug(f"Processing {event}")
    if event.user.name is None or event.user.name.strip() == "":
        try:
            ts_client.delete(f"profile_{event.user.id}")
        except ObjectNotFound:
            pass
    else:
        ts_client.upsert(event.as_typesense_document())
    logger.info(f"Upserted user {event.user.id} in Typesense")


def process_user_deleted_event(ts_client: TypesenseClient, event: UserDeletedEvent) -> None:
    logger.debug(f"Processing {event}")
    try:
        ts_client.delete(f"profile_{event.user.id}")
    except ObjectNotFound:
        pass
    logger.info(f"Deleted user {event.user.id} from Typesense")


def process_space_upserting_event(
    ts_client: TypesenseClient, event: Union[SpaceCreatedEvent, SpaceUpdatedEvent]
) -> None:
    logger.debug(f"Processing {event}")
    ts_client.upsert(event.as_typesense_document())
    logger.info(f"Upserted space {event.space.id} in Typesense")


def process_space_deleted_event(ts_client: TypesenseClient, event: SpaceDeletedEvent) -> None:
    logger.debug(f"Processing {event}")
    try:
        ts_client.delete(f"space_{event.space.id}")
    except ObjectNotFound:
        pass
    logger.info(f"Deleted space {event.space.id} in Typesense")


@dataclasses.dataclass
class TaskClassificationItem(ClassificationItem):
    id: str
    hash: str
    topics_skills_embedding_v2: Optional[List[float]]
    topics_v2: Optional[List[str]]
    skills_v2: Optional[List[str]]
    similarity_embedding: Optional[List[float]]

    # makes mypy type checking happy (means that the data may have arbitrary other fields not listed above)
    def __getattr__(self, name: str) -> Any: ...


def process_task_upserting_event(ts_client: TypesenseClient, event: Union[TaskCreatedEvent, TaskUpdatedEvent]) -> None:
    logger.debug(f"Processing {event}")
    vertexai.init(project=gcloud_project_id(), location=gcloud_location())
    bq_client = bigquery.Client(project=gcloud_project_id(), location=gcloud_location())
    classifier = TaskClassifier(bigquery_table_name_classifications(), logger, bq_client)
    item = TaskClassificationItem(
        id=event.task.id,
        hash=event.farm_fingerprint(),
        topics_skills_embedding_v2=None,
        topics_v2=event.space.topics_v2,
        skills_v2=event.task.skills_v2,
        similarity_embedding=None,
    )
    classification = classifier.get_cached_classification(item)
    if not classification:
        classification = classifier.classify_item_and_cache_classification(item)
    ts_client.upsert(event.as_typesense_document(classification))
    logger.info(f"Upserted task {event.task.id} in Typesense")


def process_task_deleted_event(ts_client: TypesenseClient, event: TaskDeletedEvent) -> None:
    logger.debug(f"Processing {event}")
    try:
        ts_client.delete(f"task_{event.task.id}")
    except ObjectNotFound:
        pass
    logger.info(f"Deleted task {event.task.id} in Typesense")


def process_event(ts_client: TypesenseClient, event: CloudEvent) -> None:
    match get_event_type(event):
        case "UserUpdated":
            return process_user_updated_event(ts_client, UserUpdatedEvent(event))
        case "UserDeleted":
            return process_user_deleted_event(ts_client, UserDeletedEvent(event))
        case "UserNameUpdated":
            # no need to process, it is sufficient to handle UserUpdated
            logger.debug("Ignoring UserNameUpdated event")
            return None
        case "UserEmailUpdated":
            # no need to process, it is sufficient to handle UserUpdated
            # also, no PII must be in Typesense as there's no authentication
            logger.debug("Ignoring UserEmailUpdated event")
            return None
        case "SpaceCreated":
            return process_space_upserting_event(ts_client, SpaceCreatedEvent(event))
        case "SpaceUpdated":
            return process_space_upserting_event(ts_client, SpaceUpdatedEvent(event))
        case "SpaceDeleted":
            return process_space_deleted_event(ts_client, SpaceDeletedEvent(event))
        case "TaskCreated":
            return process_task_upserting_event(ts_client, TaskCreatedEvent(event))
        case "TaskUpdated":
            return process_task_upserting_event(ts_client, TaskUpdatedEvent(event))
        case "TaskDeleted":
            return process_task_deleted_event(ts_client, TaskDeletedEvent(event))
        case (eventType, eventVersion):
            # other events than handled above are assumed to (currently) not be of interest in search
            logger.debug(f"Ignoring {eventType} ({eventVersion}) event.")
            return


@functions_framework.cloud_event
def process_message(event: CloudEvent) -> None:
    ts_client = TypesenseClient()
    process_event(ts_client, event)
