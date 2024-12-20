from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict

import prefect
from dateutil.parser import isoparse

from src.common.variables import variable_value_as, update_variable

logger = prefect.logging.get_logger()


@dataclass
class CrawlerState:
    last_successful_day: date | None
    last_run_day: date | None
    variable_key: str

    @staticmethod
    def _from_variable(variable_key: str, value: Dict[str, str | None]) -> CrawlerState:
        date_values = {k: isoparse(v).date() if v else None for (k, v) in value.items()}
        return CrawlerState(
            last_successful_day=date_values.get("last_successful_day", None),
            last_run_day=date_values.get("last_run_day", None),
            variable_key=variable_key,
        )

    @staticmethod
    def get(variable_key: str) -> CrawlerState:
        state_from_prefect = variable_value_as(dict[str, str | None], variable_key, default={})
        return CrawlerState._from_variable(variable_key, state_from_prefect)

    def persist(self) -> None:
        serializable_state: Dict[str, str | None] = {
            "last_successful_day": self.last_successful_day.isoformat() if self.last_successful_day else None,
            "last_run_day": self.last_run_day.isoformat() if self.last_run_day else None,
        }
        update_variable(self.variable_key, serializable_state)
