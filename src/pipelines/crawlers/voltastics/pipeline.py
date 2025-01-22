from __future__ import annotations

import asyncio
from asyncio import Queue
from typing import Any, Dict

import prefect
from prefect import task

from src.common.extractors.rest_crawler import RestCrawler, PagingInfo
from src.common.loaders.bq_loader import is_streaming_buffer_empty, delete_partition
from src.common.monitoring.alerting import alerting_flow
from src.common.variables import variable_value_as, get_secret_value
from src.pipelines.crawlers.crawler_common_code import (
    update_last_successful_day,
    source_to_sink,
    bigquery_sink,
    has_run_successfully_for_day,
    is_cleanup_needed,
)
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


# TODO can we somehow avoid silencing mypy here?
@alerting_flow(context=_CONFIG)  # type: ignore
def crawl_voltastics(force_run: bool = False) -> None:
    if not has_run_successfully_for_day(_CONFIG) or force_run:
        logger.info("Crawling did not run successfully for today, starting run.")

        if is_cleanup_needed(_CONFIG):
            if is_streaming_buffer_empty(*_CONFIG.get_bq_table_fqdn()) or force_run:
                logger.info("Unfinished run happened for today, clearing today's partition.")
                delete_partition(*_CONFIG.get_bq_table_fqdn(), partition=_CONFIG.partition_date)
            else:
                logger.warning("Unfinished run happened for today, but streaming buffer is not empty. Aborting flow.")
                return

        asyncio.run(source_to_sink(crawl, bigquery_sink(_CONFIG, _STOP_DOC)))
        update_last_successful_day(_CONFIG)  # = finalize
    else:
        logger.info("Crawling already ran successfully for today, skipping run.")


if __name__ == "__main__":
    crawl_voltastics(True)
