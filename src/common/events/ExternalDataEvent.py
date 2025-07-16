from dataclasses import dataclass
from typing import Dict, Any


# ruff: noqa: N999 ???
@dataclass
class ExternalDataEvent:
    def __init__(self, event_type: str, event_version: str, data: Dict[str, Any]):
        self.event_type = event_type
        self.event_version = event_version
        self.data = data

    def to_dict(self) -> Dict[str, Any]:
        return {
            "eventType": self.event_type,
            "eventVersion": self.event_version,
            "data": self.data,
        }
