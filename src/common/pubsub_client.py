import json

from google.cloud import pubsub_v1  # type: ignore[attr-defined]

from shared.events.event import Event
from src.common.variables import variable_value_as


class PubSubClient:
    def __init__(self, topic_name: str) -> None:
        project_id = variable_value_as(str, "google_cloud_project_id")

        self.publisher = pubsub_v1.PublisherClient()
        self.topic_name = f"projects/{project_id}/topics/{topic_name}"

    def publish(self, event: Event) -> pubsub_v1.publisher.futures.Future:
        data = json.dumps(event.to_dict()).encode("utf-8")
        return self.publisher.publish(self.topic_name, data)
