from typing import cast, Dict


from src.common.variables import variable_value_as

from dataclasses import dataclass


@dataclass
class BigQueryDatalake:
    project: str
    dataset: str

    @classmethod
    def from_env(cls) -> "BigQueryDatalake":
        data = cast(Dict[str, str], variable_value_as(Dict[str, str], "bigquery_datalake"))
        return cls(project=data["project"], dataset=data["dataset"])


bigquery_datalake = BigQueryDatalake.from_env()

volunteering_voltastics_classified_enriched_partitioned: str = (
    f"{bigquery_datalake.project}.{bigquery_datalake.dataset}.volunteering_voltastics_classified_enriched_partitioned"
)
