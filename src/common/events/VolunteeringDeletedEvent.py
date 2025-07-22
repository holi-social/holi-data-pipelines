from dataclasses import dataclass

from src.common.events.ExternalDataEvent import ExternalDataEvent


# ruff: noqa: N999 ???
@dataclass
class VolunteeringDeletedEvent(ExternalDataEvent):
    def __init__(self, record_id: str):
        super().__init__(
            event_type="VolunteeringDeleted",
            event_version="1.0.0",
            data={"id": record_id},
        )
