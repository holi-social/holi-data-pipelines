from datetime import date
from typing import Callable, Any, Awaitable

import time
import prefect
from prefect import task
from codetiming import Timer
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse

logger = prefect.logging.get_logger()


async def stream_to_bigquery(
    project: str,
    dataset: str,
    table: str,
    get_document: Callable[[], Awaitable[Any]],
    batch_size: int = 1000,
    stop_document: Any = None,
) -> None:
    bq_client = GcpCredentials(project=project).get_bigquery_client()
    while True:
        finished = False
        documents = []

        for _ in range(batch_size):  # Batch size for streaming to BigQuery
            document = await get_document()
            if document == stop_document:  # Stop signal
                finished = True
                break
            documents.append(document)

        if documents:
            with Timer(
                text=f"Stream-inserting {len(documents)} documents into BigQuery took {{:0.4f}} seconds",
                logger=logger.info,
            ):
                # prefect_gcp.bigquery.bigquery_insert_stream() does not allow to set ignore_unknown_values
                response = bq_client.insert_rows_json(
                    table=f"{project}.{dataset}.{table}", ignore_unknown_values=True, json_rows=documents
                )
                errors = []
                for row in response:
                    if "errors" in row:
                        errors.append(row["errors"])

                if errors:
                    raise ValueError(errors)

        if finished:  # Stop signal
            return


def delete_partition(table_name: str, partition: date) -> None:
    with BigQueryWarehouse.load("bigquery-warehouse") as warehouse:
        warehouse.execute(
            f"DELETE from `{table_name}` where TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = "  # noqa: S608
            f"TIMESTAMP('{partition.isoformat()}')"
        )


def is_streaming_buffer_empty(table_name: str) -> bool:
    with BigQueryWarehouse.load("bigquery-warehouse") as warehouse:
        count = warehouse.fetch_one(
            f"SELECT count(*) from `{table_name}` where _PARTITIONTIME is null"  # noqa: S608
        )[0]
        return count == 0


@task(timeout_seconds=60 * 90)
def wait_for_bigquery_data_to_be_accessible(table_name: str) -> None:
    while True:
        if is_streaming_buffer_empty(table_name):
            break
        time.sleep(60)
