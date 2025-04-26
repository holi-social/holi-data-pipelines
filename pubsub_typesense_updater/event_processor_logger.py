import logging
from typing import Optional, Any

from cloudevents.http import CloudEvent

from pubsub_typesense_updater.events import (
    DomainEvent,
    get_message_id,
    get_event_type,
    get_event_version,
    UserEvent,
    SpaceEvent,
    TaskEvent,
)


class EventProcessorLogger(logging.Logger):
    cloud_event: Optional[CloudEvent] = None
    domain_event: Optional[DomainEvent] = None

    def _get_labels(self) -> dict:
        """Get labels based on cloud_event and domain_event properties."""
        labels = {}

        # Add cloud event labels if available
        if self.cloud_event:
            labels.update(
                {
                    "messageId": get_message_id(self.cloud_event),
                    "eventType": get_event_type(self.cloud_event),
                    "eventVersion": str(get_event_version(self.cloud_event)),
                }
            )

        # Add domain event specific labels
        if self.domain_event:
            # For UserEvent
            if isinstance(self.domain_event, UserEvent):
                labels["userId"] = self.domain_event.user.id

            # For SpaceEvent
            if isinstance(self.domain_event, SpaceEvent):
                labels["spaceId"] = self.domain_event.space.id

            # For TaskEvent
            if isinstance(self.domain_event, TaskEvent):
                labels["taskId"] = self.domain_event.task.id
                labels["spaceId"] = self.domain_event.space.id

        return labels

    def _add_labels_to_kwargs(self, kwargs: dict) -> dict:
        """Add labels to the extra dict in kwargs."""
        labels = self._get_labels()
        if not labels:
            return kwargs

        # Create or update the extra dict with labels
        if "extra" not in kwargs:
            kwargs["extra"] = {"labels": labels}
        elif "labels" not in kwargs["extra"]:
            kwargs["extra"]["labels"] = labels
        else:
            kwargs["extra"]["labels"].update(labels)

        return kwargs

    def debug(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        kwargs = self._add_labels_to_kwargs(kwargs)
        super().debug(msg, *args, **kwargs)

    def info(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        kwargs = self._add_labels_to_kwargs(kwargs)
        super().info(msg, *args, **kwargs)

    def warning(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        kwargs = self._add_labels_to_kwargs(kwargs)
        super().warning(msg, *args, **kwargs)

    def error(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        kwargs = self._add_labels_to_kwargs(kwargs)
        super().error(msg, *args, **kwargs)

    def critical(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        kwargs = self._add_labels_to_kwargs(kwargs)
        super().critical(msg, *args, **kwargs)

    def exception(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        kwargs = self._add_labels_to_kwargs(kwargs)
        super().exception(msg, *args, **kwargs)

    def log(self, level: int, msg: Any, *args: Any, **kwargs: Any) -> None:
        kwargs = self._add_labels_to_kwargs(kwargs)
        super().log(level, msg, *args, **kwargs)
