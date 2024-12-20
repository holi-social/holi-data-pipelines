from __future__ import annotations
import asyncio
import dataclasses
import functools
from asyncio import Queue
from copy import copy
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, TypedDict, Self, Tuple, Iterable

from dateutil.parser import isoparse
import prefect
from prefect import flow, task, State
from prefect.types import StrictVariableValue
from prefect_gcp import BigQueryWarehouse

from src.common.extractors.rest_crawler import RestCrawler
from src.common.loaders.bq_loader import stream_to_bigquery, delete_partition, insert
from src.common.monitoring.alerting import alert
from src.common.variables import variable_value_as, get_secret_value, update_variable

logger = prefect.logging.get_logger()


@dataclass
class VoltasticsState:
    last_successful_day: date | None
    last_run_day: date | None
    variable_key = "voltastics_crawler_state"

    @staticmethod
    def _from_variable(value: Dict[str, str | None]) -> VoltasticsState:
        date_values = {k: isoparse(v).date() if v else None for (k, v) in value.items()}
        return VoltasticsState(
            last_successful_day=date_values.get("last_successful_day", None),
            last_run_day=date_values.get("last_run_day", None),
        )

    @staticmethod
    def get() -> VoltasticsState:
        state_from_prefect = variable_value_as(dict[str, str | None], VoltasticsState.variable_key, default={})
        return VoltasticsState._from_variable(state_from_prefect)

    @staticmethod
    def persist(value: VoltasticsState) -> None:
        serializable_state: Dict[str, str | None] = {
            "last_successful_day": value.last_successful_day.isoformat() if value.last_successful_day else None,
            "last_run_day": value.last_run_day.isoformat() if value.last_run_day else None,
        }
        update_variable(VoltasticsState.variable_key, serializable_state)


@dataclass
class Config:
    api_endpoint_url: str
    bigquery_table: Dict[str, str]


_CONFIG: Config = Config(
    api_endpoint_url=variable_value_as(str, "voltastics_api_endpoint_url"),
    bigquery_table=variable_value_as(Dict[str, str], "voltastics_crawler_table"),
)

_HEADERS: Dict[str, str] = {"x-voltastics-token": get_secret_value("voltastics-api-key")}


# dateutil.parser.parse(datetime.datetime.now().isoformat())


@task
def clear_partition(partition: date = datetime.today()) -> None:
    project = _CONFIG.bigquery_table["project"]
    dataset = _CONFIG.bigquery_table["dataset"]
    table = _CONFIG.bigquery_table["table"]

    delete_partition(project, dataset, table, partition)


@task
async def crawl(queue: Queue[Any]) -> None:
    def add_page_to_queue(page: Any) -> None:
        if page is None:  # stop signal, last element
            queue.put_nowait(None)
        else:
            for document in page.get("entries"):
                queue.put_nowait(document)

    crawler = RestCrawler(
        str(_CONFIG.api_endpoint_url),
        lambda index_and_size: {
            "offset": index_and_size[0] * index_and_size[1],
            "limit": index_and_size[1],
            "city": "ALL_CITIES",
        },
        lambda page: page.get("totalEntries", 0),
        add_page_to_queue,
        headers=_HEADERS,
    )

    await crawler.crawl()


@task
async def write_to_bigquery(queue: Queue[Any], batch_size: int = 1000) -> None:
    async def get_document_from_queue() -> Any:
        result = await queue.get()
        queue.task_done()
        return result

    project = _CONFIG.bigquery_table["project"]
    dataset = _CONFIG.bigquery_table["dataset"]
    table = _CONFIG.bigquery_table["table"]

    # await stream_to_bigquery(project, dataset, table, get_document=get_document_from_queue)
    while True:
        finished = False
        documents = []

        for _ in range(batch_size):  # Batch size for streaming to BigQuery
            document = await queue.get()
            if document == None:  # Stop signal
                finished = True
                break
            documents.append(document)

        if documents:
            insert(project, dataset, table, documents)

            logger.info(f"Inserted {len(documents)} documents into BigQuery")
        if finished:  # Stop signal
            return


async def async_crawl() -> None:
    # documents are transported from the crawler to the writer via this queue
    queue: Queue[Any] = asyncio.Queue()
    tasks = [asyncio.create_task(crawl(queue)), asyncio.create_task(write_to_bigquery(queue))]
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.exception("gather hit exception")
        for t in tasks:
            t.cancel()
            raise e


@flow(
    on_failure=[functools.partial(alert, vars(_CONFIG))],
    on_crashed=[functools.partial(alert, vars(_CONFIG))],
    on_cancellation=[functools.partial(alert, vars(_CONFIG))],
)
def crawl_voltastics() -> None:
    initial_state = VoltasticsState.get()
    today = datetime.today().date()
    if initial_state.last_successful_day == today:
        logger.info("Crawling already ran successfully for today, skipping run.")
    else:
        logger.info("Crawling did not run successfully for today, starting run.")

        starting_state = copy(initial_state)
        starting_state.last_run_day = today
        needs_cleanup = initial_state.last_run_day == today
        if needs_cleanup:
            logger.info("Unfinished run happened for today, clearing today's partition .")
            clear_partition(today)
        else:
            VoltasticsState.persist(starting_state)

        asyncio.run(async_crawl())

        # persist_state(copy.replace(starting_state, {"last_successful_day": today}))  # type: ignore[attr-defined]
        logger.info("finished crawling")


if __name__ == "__main__":
    crawl_voltastics()
