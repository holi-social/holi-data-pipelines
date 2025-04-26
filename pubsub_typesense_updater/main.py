import os
import logging
from typing import Union

import vertexai
import functions_framework
from google.cloud import bigquery, logging as gcloud_logging
from cloudevents.http.event import CloudEvent
from event_processor_logger import EventProcessorLogger
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
from pubsub_typesense_updater.events import DomainEvent

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


def process_task_upserting_event(ts_client: TypesenseClient, event: Union[TaskCreatedEvent, TaskUpdatedEvent]) -> None:
    logger.debug(f"[id={event.task.id}] Processing event")
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


def process_task_deleted_event(ts_client: TypesenseClient, event: TaskDeletedEvent) -> None:
    logger.debug(f"[id={event.task.id}] Processing event")
    try:
        ts_client.delete(f"task_{event.task.id}")
    except ObjectNotFound:
        pass
    logger.info(f"[id={event.task.id}] Deleted task in Typesense")


def process_event(ts_client: TypesenseClient, cloud_event: CloudEvent) -> None:
    def add_logging_context(event: DomainEvent):
        if isinstance(logger, EventProcessorLogger):
            logger.domain_event = event

    match get_event_type(cloud_event):
        case "UserUpdated":
            event = UserUpdatedEvent(cloud_event)
            add_logging_context(event)
            return process_user_updated_event(ts_client, event)
        case "UserDeleted":
            event = UserDeletedEvent(cloud_event)
            add_logging_context(event)
            return process_user_deleted_event(ts_client, event)
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
            event = SpaceCreatedEvent(cloud_event)
            add_logging_context(event)
            return process_space_upserting_event(ts_client, event)
        case "SpaceUpdated":
            event = SpaceUpdatedEvent(cloud_event)
            add_logging_context(event)
            return process_space_upserting_event(ts_client, event)
        case "SpaceDeleted":
            event = SpaceDeletedEvent(cloud_event)
            add_logging_context(event)
            return process_space_deleted_event(ts_client, event)
        case "TaskCreated":
            event = TaskCreatedEvent(cloud_event)
            add_logging_context(event)
            return process_task_upserting_event(ts_client, event)
        case "TaskUpdated":
            event = TaskUpdatedEvent(cloud_event)
            add_logging_context(event)
            return process_task_upserting_event(ts_client, event)
        case "TaskDeleted":
            event = TaskDeletedEvent(cloud_event)
            add_logging_context(event)
            return process_task_deleted_event(ts_client, event)
        case (eventType, eventVersion):
            # other events than handled above are assumed to (currently) not be of interest in search
            logger.debug(f"Ignoring {eventType} ({eventVersion}) event.")
            return


@functions_framework.cloud_event
def process_message(event: CloudEvent) -> None:
    ts_client = TypesenseClient()
    if isinstance(logger, EventProcessorLogger):
        logger.cloud_event = event
    process_event(ts_client, event)
