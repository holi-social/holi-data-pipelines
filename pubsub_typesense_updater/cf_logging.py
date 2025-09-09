import os
import logging

from google.cloud import logging as gcloud_logging
from event_processor_logger import EventProcessorLogger
from events import DomainEvent
from shared.events import DonationCreatedEvent, DonationUpdatedEvent, DonationDeletedEvent

logging.setLoggerClass(EventProcessorLogger)

# only log to Google Cloud Logging if run in that environment
if "K_SERVICE" in os.environ:
    gcloud_logging_client = gcloud_logging.Client()  # type: ignore
    gcloud_logging_client.setup_logging(log_level=logging.INFO)  # type: ignore
else:
    # configure console logging for local development
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)

logger = logging.getLogger("pubsub_typesense_updater")
logger.setLevel(logging.DEBUG)

# add console handler for local development if not in cloud environment
if "K_SERVICE" not in os.environ:
    logger.addHandler(console_handler)


def add_logging_context(
    event: DomainEvent | DonationCreatedEvent | DonationUpdatedEvent | DonationDeletedEvent,
) -> None:
    if isinstance(logger, EventProcessorLogger):
        logger.domain_event = event
