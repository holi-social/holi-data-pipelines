import sys
import logging

from datetime import date
from typing import List

from prefect import flow
from prefect.states import State, Failed, Completed

from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.common.loaders.typesense_loader import upsert_typesense_documents
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import (
    on_cancellation_hooks,
    on_crashed_hooks,
    on_failure_hooks,
    on_completion_hooks,
)
from src.common.typesense import extract_collection_name_from_schema
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.pipelines.bigquery import volunteering_voltastics_classified_enriched_partitioned
from src.pipelines.typesense import schema_path
from src.pipelines.volunteering_to_typesense.tasks import transform_data

logger = PipelineLogger(__name__)


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def voltastics_to_typesense_ingest_by_ids_flow(ids: List[int], partition_date: date) -> State:
    typesense_client = TypesenseClient(TypesenseClientConfig.from_env())
    alias_name = extract_collection_name_from_schema(schema_path=schema_path())
    # ruff: noqa: S608
    bq_query = f"""
    SELECT *
    FROM {volunteering_voltastics_classified_enriched_partitioned()}
    WHERE
      TIMESTAMP_TRUNC(partitiontime, DAY) = TIMESTAMP('{partition_date.isoformat()}')
      AND id IN (
        {",\n".join([str(id) for id in ids])}
      )
    """
    logger.debug(f"BigQuery query >>>\n{bq_query}")
    bq_result = extract_data_from_bigquery(bq_query)
    logger.debug(f"BiqQuery result >>>\n{bq_result}")
    ts_documents = transform_data(bq_result)
    logger.debug(f"Typesense documents >>>\n{ts_documents}")
    if upsert_typesense_documents(typesense_client, logger, alias_name, ts_documents):
        return Completed(msg="Typesense document successfully upserted")
    else:
        return Failed(msg="Error upserting Typesense document")


if __name__ == "__main__":
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    ids = [int(id_str.strip()) for id_str in sys.argv[1].split(",")]
    partition_date = date.fromisoformat(sys.argv[2]) if len(sys.argv) >= 3 else date.today()
    voltastics_to_typesense_ingest_by_ids_flow(ids, partition_date)  # type: ignore
