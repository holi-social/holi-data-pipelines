from datetime import date
from typing import Callable, Any, Awaitable, Dict, List

import prefect
from codetiming import Timer
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse
from typing_extensions import deprecated

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


@deprecated("Slow one-after-another insert-via-statement, use streaming instead")
async def insert(project: str, dataset: str, table: str, documents: List[Dict[str, Any]]) -> None:
    """
    Inserts documents into bigquery one by one. Very slow, consider using streaming.

    :param project:
    :param dataset:
    :param table:
    :param documents:
    :return:
    """
    with await BigQueryWarehouse.load("bigquery-warehouse") as warehouse:
        schema: list[dict[str, str]] = (
            await warehouse.fetch_one(
                f"""
                SELECT 
                    ARRAY_AGG(STRUCT( 
                      IF(is_nullable = 'YES', 'NULLABLE', 'REQUIRED') AS mode,
                      column_name AS name,
                      data_type AS type)
                    ORDER BY ordinal_position) AS schema
                FROM
                  `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
                WHERE
                  table_name = '{table}'
                """
            )
        )[0]
        schema_by_name = {item["name"]: item["type"] for item in schema}
        names = list(documents[0].keys())
        column_names = ",".join(names)
        bv_names = ",".join(f"%({n}:{schema_by_name[n]})s" for n in names)
        await warehouse.execute_many(
            f"INSERT into `{project}.{dataset}.{table}` ({column_names}) values ({bv_names})",  # noqa: S608
            seq_of_parameters=documents,
        )


def delete_partition(project: str, dataset: str, table: str, partition: date) -> None:
    with BigQueryWarehouse.load("bigquery-warehouse") as warehouse:
        warehouse.execute(
            f"DELETE from `{project}.{dataset}.{table}` where TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = "  # noqa: S608
            f"TIMESTAMP('{partition.isoformat()}')"
        )


def is_streaming_buffer_empty(project: str, dataset: str, table: str) -> bool:
    with BigQueryWarehouse.load("bigquery-warehouse") as warehouse:
        count = warehouse.fetch_one(
            f"SELECT count(*) from `{project}.{dataset}.{table}` where _PARTITIONTIME is null"  # noqa: S608
        )[0]
        return count == 0
