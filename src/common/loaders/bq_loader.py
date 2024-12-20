from datetime import date
from typing import Callable, Any, Awaitable, Dict, List

import prefect
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_insert_stream, BigQueryWarehouse

logger = prefect.logging.get_logger()


async def stream_to_bigquery(
    project: str, dataset: str, table: str, get_document: Callable[[], Awaitable[Any]], stop_document: Any = None
) -> None:
    while True:
        finished = False
        documents = []

        for _ in range(1000):  # Batch size for streaming to BigQuery
            document = await get_document()
            if document == stop_document:  # Stop signal
                finished = True
                break
            documents.append(document)

        if documents:
            bigquery_insert_stream(
                dataset=dataset,
                table=table,
                records=documents,
                gcp_credentials=GcpCredentials(project=project),
            )

            logger.info(f"Inserted {len(documents)} documents into BigQuery")
        if finished:  # Stop signal
            return


def insert(project: str, dataset: str, table: str, documents: List[Dict[str, Any]]) -> None:
    with BigQueryWarehouse.load("bigquery-warehouse") as warehouse:
        warehouse.execute_many(
            f"INSERT into `{project}.{dataset}.{table}` (longitude, imageOrga, location, title, source, hashTags, description, "
            f"image, kink, orga, id, latitide, uses) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            seq_of_parameters=documents,
        )


#    { "name": "longitude", "type": "FLOAT", "mode": "NULLABLE" },
#    { "name": "imageOrga", "type": "STRING", "mode": "NULLABLE"},
#    { "name": "location",  "type": "STRING", "mode": "NULLABLE"},
#    { "name": "title","type": "STRING","mode": "NULLABLE"},
#    { "name": "source", "type": "STRING", "mode": "NULLABLE"},
#    { "name": "hashTags", "type": "STRING", "mode": "REPEATED"},
#    { "name": "description", "type": "STRING","mode": "NULLABLE"},
#    { "name": "image", "type": "STRING","mode": "NULLABLE"},
#    { "name": "link", "type": "STRING", "mode": "NULLABLE"},
#    { "name": "orga", "type": "STRING", "mode": "NULLABLE"},
#    { "name": "id", "type": "INTEGER","mode": "NULLABLE"},
#    { "name": "latitude","type": "FLOAT", "mode": "NULLABLE"},
#    { "name": "uses", "type": "STRING","mode": "REPEATED"}


def delete_partition(project: str, dataset: str, table: str, partition: date) -> None:
    with BigQueryWarehouse.load("bigquery-warehouse") as warehouse:
        warehouse.execute(
            f"DELETE from `{project}.{dataset}.{table}` where TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP('{partition.isoformat()}')"
        )
