import re
from logging import getLogger
from typing import Union

import functions_framework
from cloudevents.http.event import CloudEvent
from typesense.exceptions import ObjectNotFound

from events import (
    get_event_type,
    get_event_version,
    UserUpdatedEvent,
    UserDeletedEvent,
    SpaceCreatedEvent,
    SpaceUpdatedEvent,
    SpaceDeletedEvent,
)
from shared.hello_shared import hello_shared
from typesense_client import TypesenseClient

logger = getLogger(__name__)

SEMVER_1XX_SERIES = r"^1\.\d+\.\d+$"


def process_user_updated_event(client: TypesenseClient, event: UserUpdatedEvent) -> None:
    logger.debug(f"Processing {event}")
    if event.user.name is None or event.user.name.strip() == "":
        try:
            client.delete(f"profile_{event.user.id}")
        except ObjectNotFound:
            pass
    else:
        client.upsert(event.as_typesense_document())
    logger.info(f"Upserted user {event.user.id} in Typesense")


def process_user_deleted_event(client: TypesenseClient, event: UserDeletedEvent) -> None:
    logger.debug(f"Processing {event}")
    try:
        client.delete(f"profile_{event.user.id}")
    except ObjectNotFound:
        pass
    logger.info(f"Deleted user {event.user.id} from Typesense")


def process_space_upserting_event(client: TypesenseClient, event: Union[SpaceCreatedEvent, SpaceUpdatedEvent]) -> None:
    logger.debug(f"Processing {event}")
    client.upsert(event.as_typesense_document())
    logger.info(f"Upserted space {event.space.id} in Typesense")


def process_space_deleted_event(client: TypesenseClient, event: SpaceDeletedEvent) -> None:
    logger.debug(f"Processing {event}")
    try:
        client.delete(f"space_{event.space.id}")
    except ObjectNotFound:
        pass
    logger.info(f"Deleted space {event.space.id} in Typesense")


def process_event(client: TypesenseClient, event: CloudEvent) -> None:
    type_version = (get_event_type(event), re.match(SEMVER_1XX_SERIES, get_event_version(event)) is not None)
    match type_version:
        case ("UserUpdated", True):
            return process_user_updated_event(client, UserUpdatedEvent(event))
        case ("UserDeleted", True):
            return process_user_deleted_event(client, UserDeletedEvent(event))
        case ("UserNameUpdated", True):
            # no need to process, it is sufficient to handle UserUpdated
            logger.debug("Ignoring UserNameUpdated event")
            return None
        case ("UserEmailUpdated", True):
            # no need to process, it is sufficient to handle UserUpdated
            # also, no PII must be in Typesense as there's no authentication
            logger.debug("Ignoring UserEmailUpdated event")
            return None
        case ("SpaceCreated", True):
            return process_space_upserting_event(client, SpaceCreatedEvent(event))
        case ("SpaceUpdated", True):
            return process_space_upserting_event(client, SpaceUpdatedEvent(event))
        case ("SpaceDeleted", True):
            return process_space_deleted_event(client, SpaceDeletedEvent(event))
        case (eventType, eventVersion):
            # other events than handled above are assumed to (currently) not be of interest in search
            logger.debug(f"Ignoring {eventType} ({eventVersion}) event.")
            return


@functions_framework.cloud_event
def process_message(event: CloudEvent) -> None:
    client = TypesenseClient()
    process_event(client, event)
    hello_shared()


# TODO ensure that UserUpdated or similar is emitted on updates of fields we're interested in
