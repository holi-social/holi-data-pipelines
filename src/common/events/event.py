from typing import Protocol
from semver import Version


class Event(Protocol):
    event_type: str
    event_version: Version

    def to_dict(self) -> str: ...
