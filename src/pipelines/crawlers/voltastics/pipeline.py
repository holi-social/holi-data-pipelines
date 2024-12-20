from __future__ import annotations

import asyncio
import functools
from asyncio import Queue
from datetime import date, datetime
from typing import Any, Dict, Optional

import prefect
from prefect import flow, task

from src.common.extractors.rest_crawler import RestCrawler
from src.common.loaders.bq_loader import delete_partition, stream_to_bigquery, is_streaming_buffer_empty
from src.common.monitoring.alerting import alert
from src.common.variables import variable_value_as, get_secret_value
from src.pipelines.crawlers.crawler_config import CrawlerConfig
from src.pipelines.crawlers.crawler_state import CrawlerState

logger = prefect.logging.get_logger()


_CONFIG: CrawlerConfig = CrawlerConfig(
    api_endpoint_url=variable_value_as(str, "voltastics_api_endpoint_url"),
    bigquery_table=variable_value_as(Dict[str, str], "voltastics_crawler_table"),
)

_HEADERS: Dict[str, str] = {"x-voltastics-token": get_secret_value("voltastics-api-key")}


@task
def clear_partition(partition: Optional[date]) -> None:
    partition_to_delete: date = partition or datetime.today()
    delete_partition(*_CONFIG.get_bq_table_fqdn(), partition_to_delete)


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
async def write_to_bigquery(queue: Queue[Any]) -> None:
    async def get_document_from_queue() -> Any:
        result = await queue.get()
        queue.task_done()
        return result

    await stream_to_bigquery(*_CONFIG.get_bq_table_fqdn(), get_document=get_document_from_queue)


async def async_crawl() -> None:
    # documents are transported from the crawler to the writer via this queue
    queue: Queue[Any] = asyncio.Queue()
    async with asyncio.TaskGroup() as group:
        group.create_task(crawl(queue))
        group.create_task(write_to_bigquery(queue))


@flow(
    on_failure=[functools.partial(alert, vars(_CONFIG))],
    on_crashed=[functools.partial(alert, vars(_CONFIG))],
    on_cancellation=[functools.partial(alert, vars(_CONFIG))],
)
def crawl_voltastics(force: bool = False) -> None:
    state = CrawlerState.get("voltastics_crawler_state")
    today = datetime.today().date()
    if state.last_successful_day == today and not force:
        logger.info("Crawling already ran successfully for today, skipping run.")
    else:
        logger.info("Crawling did not yet run successfully for today, starting run.")

        needs_cleanup = state.last_run_day == today
        if needs_cleanup:
            if is_streaming_buffer_empty(*_CONFIG.get_bq_table_fqdn()):
                logger.info("Unfinished run happened for today, clearing today's partition.")
                clear_partition(today)
            else:
                logger.warning("Unfinished run happened for today, but streaming buffer is not empty. Aborting flow.")
                return

        state.last_run_day = today
        state.persist()

        asyncio.run(async_crawl())

        state.last_successful_day = today
        state.persist()
        logger.info("finished crawling")


if __name__ == "__main__":
    crawl_voltastics(True)
