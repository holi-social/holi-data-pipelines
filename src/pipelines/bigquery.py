from dataclasses import dataclass
from typing import Dict, cast
from functools import cache

from src.common.variables import variable_value_as


@dataclass
class BigQueryDataset:
    project: str
    dataset: str

    @classmethod
    def from_env(cls, key: str) -> "BigQueryDataset":
        data = cast(Dict[str, str], variable_value_as(Dict[str, str], key))
        return cls(project=data["project"], dataset=data["dataset"])

    def __str__(self) -> str:
        return f"{self.project}.{self.dataset}"


# !!! PLEASE NOTE !!!
# dataset and table names herein are defined as function so they are evaluated lazily when application code needs it
# this avoids resolution at compile time which can fail when no Prefect env is present - e.g. in unit test execution
# function return values are cached forever, so they are only evaluated once


@cache
def bigquery_datalake() -> BigQueryDataset:
    return BigQueryDataset.from_env("bigquery_datalake")


@cache
def bigquery_datapool() -> BigQueryDataset:
    return BigQueryDataset.from_env("bigquery_datapool")


@cache
def bigquery_okuna_dataset() -> BigQueryDataset:
    return BigQueryDataset.from_env("bigquery_okuna_dataset")


@cache
def table_location_cache() -> str:
    return f"{bigquery_datalake()}.location_cache"


@cache
def table_classifications() -> str:
    return f"{bigquery_datalake()}.classifications"


@cache
def tasks_without_classifications() -> str:
    return f"{bigquery_datapool()}.tasks_without_classifications"


@cache
def tasks_with_classifications() -> str:
    return f"{bigquery_datapool()}.tasks_with_classifications"


@cache
def volunteering_voltastics_raw_deduped() -> str:
    return f"{bigquery_datalake()}.volunteering_voltastics_raw_deduped"


@cache
def volunteering_voltastics_without_classifications() -> str:
    return f"{bigquery_datapool()}.volunteering_voltastics_without_classifications"


@cache
def volunteering_voltastics_classified_enriched_partitioned() -> str:
    return f"{bigquery_datalake()}.volunteering_voltastics_classified_enriched_partitioned"


@cache
def volunteering_aktion_mensch_without_classifications() -> str:
    return f"{bigquery_datapool()}.volunteering_aktion_mensch_without_classifications"


@cache
def volunteering_aktion_mensch_classified_enriched_partitioned() -> str:
    return f"{bigquery_datalake()}.volunteering_aktion_mensch_classified_enriched_partitioned"
