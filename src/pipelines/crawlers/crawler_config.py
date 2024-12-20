from dataclasses import dataclass
from typing import Dict, Tuple

import prefect

logger = prefect.logging.get_logger()


@dataclass
class CrawlerConfig:
    api_endpoint_url: str
    bigquery_table: Dict[str, str]

    def get_bq_project(self) -> str:
        return self.bigquery_table["project"]

    def get_bq_dataset(self) -> str:
        return self.bigquery_table["dataset"]

    def get_bq_table(self) -> str:
        return self.bigquery_table["table"]

    def get_bq_table_fqdn(self) -> Tuple[str, str, str]:
        return self.get_bq_project(), self.get_bq_dataset(), self.get_bq_table()
