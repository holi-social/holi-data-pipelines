import os
import logging
from typing import Union

import vertexai
import functions_framework
from google.cloud import bigquery, logging as gcloud_logging
from cloudevents.http.event import CloudEvent
from event_processor_logger import EventProcessorLogger
from events import (
    DomainEvent,
    SpaceCreatedEvent,
    SpaceDeletedEvent,
    SpaceUpdatedEvent,
    TaskCreatedEvent,
    TaskDeletedEvent,
    TaskUpdatedEvent,
    UserDeletedEvent,
    UserUpdatedEvent,
    get_event_type,
    get_event_version,
)

from shared.classifiers.task_classification_input import TaskClassificationInput
from task_classifier import TaskClassifier

from typesense_client import TypesenseClient
from typesense.exceptions import ObjectNotFound

logging.setLoggerClass(EventProcessorLogger)

# only log to Google Cloud Logging if run in that environment
if "K_SERVICE" in os.environ:
    gcloud_logging_client = gcloud_logging.Client()  # type: ignore
    gcloud_logging_client.setup_logging(log_level=logging.INFO)  # type: ignore

logger = logging.getLogger("pubsub_typesense_updater")
logger.setLevel(logging.DEBUG)


def gcloud_project_id() -> str:
    return os.environ.get("GCLOUD_PROJECT_ID", "")


def gcloud_location() -> str:
    return os.environ.get("GCLOUD_LOCATION", "")


def bigquery_table_name_classifications() -> str:
    return os.environ.get("BIGQUERY_TABLE_NAME_CLASSIFICATIONS", "")


def process_user_updated_event(ts_client: TypesenseClient, event: UserUpdatedEvent) -> None:
    logger.debug(f"[id={event.user.id}] Processing event")
    if event.user.name is None or event.user.name.strip() == "":
        try:
            ts_client.delete(f"profile_{event.user.id}")
        except ObjectNotFound:
            pass
    else:
        ts_client.upsert(event.as_typesense_document())
    logger.info(f"[id={event.user.id}] Upserted user {event.user.id} in Typesense")


def process_user_deleted_event(ts_client: TypesenseClient, event: UserDeletedEvent) -> None:
    logger.debug(f"[id={event.user.id}] Processing event")
    try:
        ts_client.delete(f"profile_{event.user.id}")
    except ObjectNotFound:
        pass
    logger.info(f"[id={event.user.id}] Deleted user {event.user.id} from Typesense")


def process_space_upserting_event(
    ts_client: TypesenseClient, event: Union[SpaceCreatedEvent, SpaceUpdatedEvent]
) -> None:
    logger.debug(f"[id={event.space.id}] Processing event")
    ts_client.upsert(event.as_typesense_document())
    logger.info(f"[id={event.space.id}] Upserted space {event.space.id} in Typesense")


def process_space_deleted_event(ts_client: TypesenseClient, event: SpaceDeletedEvent) -> None:
    logger.debug(f"[id={event.space.id}] Processing event")
    try:
        ts_client.delete(f"space_{event.space.id}")
    except ObjectNotFound:
        pass
    logger.info(f"[id={event.space.id}] Deleted space {event.space.id} in Typesense")


def _upsert_task(ts_client: TypesenseClient, event: Union[TaskCreatedEvent, TaskUpdatedEvent]) -> None:
    vertexai.init(project=gcloud_project_id(), location=gcloud_location())
    bq_client = bigquery.Client(project=gcloud_project_id(), location=gcloud_location())
    classifier = TaskClassifier(bigquery_table_name_classifications(), logger, bq_client)
    item = TaskClassificationInput(
        id=event.task.id,
        hash=event.farm_fingerprint(),
        name=event.task.name,
        description=event.task.description,
        topics_skills_embedding_v2=None,
        topics_v2=event.space.topics_v2,
        skills_v2=event.task.skills_v2,
        similarity_embedding=None,
        location=event.task.location,
    )
    classification = classifier.get_cached_classification(item)
    if not classification:
        classification = classifier.classify_item_and_cache_classification(item)
    ts_client.upsert(event.as_typesense_document(classification))
    logger.info(f"[id={event.task.id}] Upserted task in Typesense")


def process_task_created_event(ts_client: TypesenseClient, event: TaskCreatedEvent) -> None:
    logger.debug(f"[id={event.task.id}] Processing TaskCreated event")
    if event.task.is_private():
        return None
    else:
        return _upsert_task(ts_client, event)


def process_task_updated_event(ts_client: TypesenseClient, event: TaskUpdatedEvent) -> None:
    logger.debug(f"[id={event.task.id}] Processing TaskUpdated event")
    if event.task.visibility == "PRIVATE":
        return _delete_task(ts_client, event.task.id)
    else:
        return _upsert_task(ts_client, event)


def _delete_task(ts_client: TypesenseClient, id: str) -> None:
    try:
        ts_client.delete(f"task_{id}")
    except ObjectNotFound:
        pass
    logger.info(f"[id={id}] Deleted task in Typesense")


def process_task_deleted_event(ts_client: TypesenseClient, id: str) -> None:
    logger.debug(f"[id={id}] Processing event")
    _delete_task(ts_client, id)


def process_event(ts_client: TypesenseClient, cloud_event: CloudEvent) -> None:
    def add_logging_context(event: DomainEvent) -> None:
        if isinstance(logger, EventProcessorLogger):
            logger.domain_event = event

    event_type = get_event_type(cloud_event)
    event_version = get_event_version(cloud_event)

    if event_type == "UserUpdated":
        user_updated_event = UserUpdatedEvent(cloud_event)
        add_logging_context(user_updated_event)
        return process_user_updated_event(ts_client, user_updated_event)
    elif event_type == "UserDeleted":
        user_deleted_event = UserDeletedEvent(cloud_event)
        add_logging_context(user_deleted_event)
        return process_user_deleted_event(ts_client, user_deleted_event)
    elif event_type == "UserNameUpdated":
        # no need to process, it is sufficient to handle UserUpdated
        logger.debug("Ignoring UserNameUpdated event")
        return None
    elif event_type == "UserEmailUpdated":
        # no need to process, it is sufficient to handle UserUpdated
        # also, no PII must be in Typesense as there's no authentication
        logger.debug("Ignoring UserEmailUpdated event")
        return None
    elif event_type == "SpaceCreated":
        space_created_event = SpaceCreatedEvent(cloud_event)
        add_logging_context(space_created_event)
        return process_space_upserting_event(ts_client, space_created_event)
    elif event_type == "SpaceUpdated":
        space_updated_event = SpaceUpdatedEvent(cloud_event)
        add_logging_context(space_updated_event)
        return process_space_upserting_event(ts_client, space_updated_event)
    elif event_type == "SpaceDeleted":
        space_deleted_event = SpaceDeletedEvent(cloud_event)
        add_logging_context(space_deleted_event)
        return process_space_deleted_event(ts_client, space_deleted_event)
    elif event_type == "TaskCreated":
        task_created_event = TaskCreatedEvent(cloud_event)
        add_logging_context(task_created_event)
        return process_task_created_event(ts_client, task_created_event)
    elif event_type == "TaskUpdated":
        task_updated_event = TaskUpdatedEvent(cloud_event)
        add_logging_context(task_updated_event)
        if task_updated_event.task.visibility == "PUBLIC":
            return process_task_updated_event(ts_client, task_updated_event)
        else:
            return process_task_deleted_event(ts_client, task_updated_event.task.id)
    elif event_type == "TaskDeleted":
        task_deleted_event = TaskDeletedEvent(cloud_event)
        add_logging_context(task_deleted_event)
        return process_task_deleted_event(ts_client, task_deleted_event.task.id)
    else:
        # other events than handled above are assumed to (currently) not be of interest in search
        logger.debug(f"Ignoring {event_type} (v{event_version}) event.")
        return


@functions_framework.cloud_event
def process_message(event: CloudEvent) -> None:
    ts_client = TypesenseClient()
    if isinstance(logger, EventProcessorLogger):
        logger.cloud_event = event
    process_event(ts_client, event)
