from asyncio import Queue
from typing import cast, Any

from aiohttp import ClientSession, TCPConnector
from google.cloud import bigquery
from google.cloud.bigquery.dbapi import DatabaseError
from prefect import flow, task
import prefect
from prefect_gcp import GcpCredentials, BigQueryWarehouse
from prefect_gcp.bigquery import bigquery_insert_stream
from src.common.variables import variable_value, secret_value

import asyncio
import json
from datetime import date, timedelta
import sys

logger = prefect.logging.get_logger()


sem = asyncio.Semaphore(1)


async def fetch_total_entries(session: ClientSession) -> int:
    params = _STATIC_PARAMS | {"offset": 0, "limit": 0}
    async with session.get("/api/searchengagement", headers=_HEADERS, params=params) as response:
        data = await response.json()
        return data.get("totalEntries", 0)  # Read totalEntries from the response


async def fetch_documents(session: ClientSession, offset: int = 0, limit: int = 100) -> list[Any]:
    params = _STATIC_PARAMS | {"offset": offset, "limit": limit}
    logger.info(f"Fetching page with offset {offset}")
    async with session.get("/api/searchengagement", headers=_HEADERS, params=params) as response:
        data = await response.json()
        logger.info(f"Fetched page with offset {offset}")
        return data.get("entries", [])  # Read totalEntries from the response


async def fetch_and_process_chunk(session: ClientSession, offset: int, limit: int, queue: Queue[Any]) -> None:
    async with sem:
        logger.info(f"Fetching chunk with offset {offset}")
        documents = await fetch_documents(session, offset, limit)
        if not documents:  # Stop if the API returns an empty list
            return
        # await save_to_disk(documents, "output.json")
        for document in documents:
            queue.put_nowait(document)


_CONFIG = {
    "api_endpoint_url": str(variable_value("voltastics_api_endpoint_url")),
    "chunk_size": 50,
}

# _SECRETS = {"voltastics_api_key": secret_value("voltastics-api-key")}

_HEADERS = {"x-voltastics-token": secret_value("voltastics-api-key") or "test"}

_STATIC_PARAMS: dict[str, str] = {"city": "ALL_CITIES"}


@task
def check_partition_exists() -> bool:
    dataset = "datalake_development"
    table = "volunteering_voltastics_raw_playground"
    today = (date.today()).strftime("%Y-%m-%d")
    with BigQueryWarehouse.load("bigquery-warehouse") as warehouse:
        result = warehouse.fetch_one(
            f"""
                    SELECT count(*) as c
                    FROM `{dataset}.{table}`
                    WHERE _PARTITIONTIME = TIMESTAMP("{today}")
                """
        )
        return result["c"] > 0


@task()
async def async_crawl() -> None:
    async with ClientSession(
        base_url=str(_CONFIG["api_endpoint_url"]),
        raise_for_status=True,
        connector=TCPConnector(limit=1, limit_per_host=1),
    ) as session:
        number_of_total_entries = await fetch_total_entries(session)
        logger.info(f"{number_of_total_entries} documents available to crawl")
        offset = 0
        tasks = []
        queue: Queue[Any] = asyncio.Queue()
        bigquery_writer = asyncio.create_task(write_to_bigquery(queue))
        while offset < number_of_total_entries:
            # Submit a task to fetch the chunk asynchronously
            logger.info(f"Scheduling fetch with offset {offset}")
            async_task = asyncio.create_task(
                fetch_and_process_chunk(session, offset, cast(int, _CONFIG["chunk_size"]), queue)
            )
            tasks.append(async_task)
            offset += cast(int, _CONFIG["chunk_size"])

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)
        await queue.put(None)  # stop signal
        await bigquery_writer
        # queue is filled with documents, right?


async def write_to_bigquery(queue: Queue[Any]) -> None:
    while True:
        finished = False
        documents = []
        for _ in range(1000):  # Batch size for streaming to BigQuery
            document = await queue.get()
            if document is None:  # Stop signal
                finished = True
                break
            documents.append(document)
            queue.task_done()

        if documents:
            try:
                result = bigquery_insert_stream(
                    dataset="datalake_development",
                    table="volunteering_voltastics_raw_playground",
                    records=documents,
                    gcp_credentials=GcpCredentials(project="holi-shared"),
                )
                logger.info(f"Inserted {len(documents)} documents into BigQuery")
            except Exception as e:
                logger.error(f"Failed to write documents to BigQuery: {e}")
                break

        if finished:  # Stop signal
            break


@flow()
def crawl_voltastics() -> None:
    if check_partition_exists() and False:
        logger.info("Partition already exists, skipping crawling")
    else:
        logger.info("Partition does not yet exist, starting crawling")
        asyncio.run(async_crawl())
        logger.info("finished crawling")


if __name__ == "__main__":
    crawl_voltastics()
