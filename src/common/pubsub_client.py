from typing import Optional
from google.cloud import pubsub_v1  # type: ignore[attr-defined]
from google.cloud.pubsub_v1.types import PublisherOptions  # type: ignore

from shared.events.event import Event, EventPayloadT
from src.common.variables import variable_value_as


class PubSubClient:
    def __init__(self, topic_name: str, ordering_key: Optional[str] = None) -> None:
        project_id = variable_value_as(str, "google_cloud_project_id")

        self.publisher = pubsub_v1.PublisherClient(
            publisher_options=PublisherOptions(enable_message_ordering=True) if ordering_key else None
        )
        self.topic_name = f"projects/{project_id}/topics/{topic_name}"
        self.ordering_key = ordering_key

    def publish(self, event: Event[EventPayloadT]) -> pubsub_v1.publisher.futures.Future:
        data = event.model_dump_json().encode("utf-8")
        return self.publisher.publish(self.topic_name, data, ordering_key=self.ordering_key)
