from dataclasses import dataclass
from typing import Dict, cast

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


bigquery_datalake = BigQueryDataset.from_env("bigquery_datalake")
bigquery_datapool = BigQueryDataset.from_env("bigquery_datapool")

bigquery_okuna_dataset = BigQueryDataset.from_env("bigquery_okuna_dataset")

table_classifications = f"{bigquery_datalake}.classifications"

tasks_without_classifications = f"{bigquery_datapool}.tasks_without_classifications"
tasks_with_classifications = f"{bigquery_datapool}.tasks_with_classifications"

volunteering_voltastics_without_classifications = (
    f"{bigquery_datapool}.volunteering_voltastics_without_classifications"
)
volunteering_voltastics_classified_enriched_partitioned: str = (
    f"{bigquery_datalake}.volunteering_voltastics_classified_enriched_partitioned"
)
