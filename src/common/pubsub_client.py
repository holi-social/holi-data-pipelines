import json

from google.cloud import pubsub_v1  # type: ignore[attr-defined]

from src.common.events.ExternalDataEvent import ExternalDataEvent
from src.common.variables import variable_value_as


class PubSubClient:
    def __init__(self) -> None:
        project_id = variable_value_as(str, "google_cloud_project_id")
        topic_name = variable_value_as(str, "google_cloud_pubsub_external_data_topic_name")

        self.publisher = pubsub_v1.PublisherClient()
        self.topic_name = f"projects/{project_id}/topics/{topic_name}"

    def publish(self, event: ExternalDataEvent) -> pubsub_v1.publisher.futures.Future:
        data = json.dumps(event.to_dict()).encode("utf-8")
        return self.publisher.publish(self.topic_name, data)
