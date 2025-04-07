from typing import cast, Dict


from src.common.variables import variable_value_as

from dataclasses import dataclass


@dataclass
class BigQueryDataset:
    project: str
    dataset: str

    @classmethod
    def from_env(cls, key: str) -> "BigQueryDataset":
        data = cast(Dict[str, str], variable_value_as(Dict[str, str], key))
        return cls(project=data["project"], dataset=data["dataset"])


bigquery_datalake = BigQueryDataset.from_env("bigquery_datalake")
bigquery_datapool = BigQueryDataset.from_env("bigquery_datapool")

volunteering_voltastics_classified_enriched_partitioned: str = (
    f"{bigquery_datalake.project}.{bigquery_datalake.dataset}.volunteering_voltastics_classified_enriched_partitioned"
)
