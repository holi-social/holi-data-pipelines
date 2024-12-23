from __future__ import annotations

import asyncio
import functools
from asyncio import Queue
from typing import Any, Dict

import prefect
from prefect import flow, task

from src.common.extractors.rest_crawler import RestCrawler, PagingInfo
from src.common.monitoring.alerting import alert
from src.common.variables import variable_value_as, get_secret_value
from src.pipelines.crawlers.crawler_common_code import check_and_setup, finalize, source_to_sink, bigquery_sink
from src.pipelines.crawlers.crawler_config import CrawlerConfig

logger = prefect.logging.get_logger()
_CONFIG: CrawlerConfig = CrawlerConfig(
    state_variable_name="voltastics_crawler_state",
    api_url=variable_value_as(str, "voltastics_api_url"),
    bigquery_table=variable_value_as(Dict[str, str], "voltastics_crawler_table"),
)
_HEADERS: Dict[str, str] = {"x-voltastics-token": get_secret_value("voltastics-api-key")}
_STOP_DOC = object()


@task
async def crawl(queue: Queue[Any]) -> None:
    def paging_params(paging_info: PagingInfo) -> dict[str, Any]:
        return {
            "offset": paging_info.offset,
            "limit": paging_info.size,
            "city": "ALL_CITIES",
        }

    async def add_page_to_queue(page: Any) -> None:
        if page is _STOP_DOC:  # stop signal, last element
            queue.put_nowait(_STOP_DOC)
        else:
            for document in page.get("entries"):
                queue.put_nowait(document)

    await RestCrawler(
        url=str(_CONFIG.api_url),
        get_params=paging_params,
        get_full_count=lambda page: page.get("totalEntries", 0),
        process_page=add_page_to_queue,
        headers=_HEADERS,
        stop_page=_STOP_DOC,
    ).crawl()


@flow(
    on_failure=[functools.partial(alert, vars(_CONFIG))],
    on_crashed=[functools.partial(alert, vars(_CONFIG))],
    on_cancellation=[functools.partial(alert, vars(_CONFIG))],
)
def crawl_voltastics(force: bool = False) -> None:
    if check_and_setup(_CONFIG, force):
        asyncio.run(source_to_sink(crawl, bigquery_sink(_CONFIG, _STOP_DOC)))
        finalize(_CONFIG)


if __name__ == "__main__":
    crawl_voltastics(True)
