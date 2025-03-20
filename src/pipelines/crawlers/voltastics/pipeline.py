from __future__ import annotations

import asyncio
from asyncio import Queue
from typing import Any, Dict, cast, Callable, Coroutine, Optional

import prefect
from prefect import task

from src.common.extractors.rest_crawler import RestCrawler, PagingInfo
from src.common.loaders.bq_loader import is_streaming_buffer_empty, delete_partition
from src.common.monitoring.flow import monitored_flow
from src.common.monitoring.metrics import report_number_of_processed_entities
from src.common.variables import variable_value_as, secret_value
from src.pipelines.crawlers.crawler_common_code import (
    update_last_successful_day,
    source_to_sink,
    bigquery_sink,
    has_run_successfully_for_day,
    is_cleanup_needed,
    update_last_run_day,
)
from src.pipelines.crawlers.crawler_config import CrawlerConfig

logger = prefect.logging.get_logger()
_CONFIG: Optional[CrawlerConfig] = None
# FIXME remove casting once https://github.com/PrefectHQ/prefect/issues/16825 has a resolution
_STOP_DOC = object()


def config() -> CrawlerConfig:
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = CrawlerConfig(
            state_variable_name="voltastics_crawler_state",
            # FIXME remove casting once https://github.com/PrefectHQ/prefect/issues/16825 has a resolution
            api_url=cast(str, variable_value_as(str, "voltastics_api_url")),
            bigquery_table=cast(Dict[str, str], variable_value_as(Dict[str, str], "voltastics_crawler_table")),
        )
    return _CONFIG


@task
def crawl() -> Callable[[Queue[Any]], Coroutine[Any, Any, int]]:
    def paging_params(paging_info: PagingInfo) -> dict[str, Any]:
        return {
            "offset": paging_info.offset,
            "limit": paging_info.size,
            "city": "ALL_CITIES",
        }

    async def crawl_to_queue(queue: Queue[Any]) -> int:
        document_count = 0

        async def add_page_to_queue(page: Any) -> None:
            nonlocal document_count

            if page is _STOP_DOC:  # stop signal, last element
                queue.put_nowait(_STOP_DOC)
            else:
                for document in page.get("entries"):
                    document_count += 1
                    queue.put_nowait(document)

        await RestCrawler(
            url=str(config().api_url),
            get_params=paging_params,
            get_full_count=lambda page: page.get("totalEntries", 0),
            process_page=add_page_to_queue,
            headers={"x-voltastics-token": cast(str, secret_value("voltastics-api-key"))},
            stop_page=_STOP_DOC,
        ).crawl()
        return document_count

    return crawl_to_queue


@monitored_flow
def crawl_voltastics(force_run: bool = False) -> None:
    # only run for days that have not been successfully crawled yet, or when forced
    if not has_run_successfully_for_day(config()) or force_run:
        logger.info("Crawling did not run successfully for today, starting run.")

        if is_cleanup_needed(config()):
            if is_streaming_buffer_empty(*config().get_bq_table_fqdn()) or force_run:
                logger.info("Unfinished run happened for today, clearing today's partition.")
                delete_partition(*config().get_bq_table_fqdn(), partition=config().partition_date)
            else:
                logger.warning("Unfinished run happened for today, but streaming buffer is not empty. Aborting flow.")
                return

        update_last_run_day(config())
        processed_entities_count = asyncio.run(source_to_sink(crawl(), bigquery_sink(config(), _STOP_DOC)))
        update_last_successful_day(config())  # = finalize
        report_number_of_processed_entities(processed_entities_count)
    else:
        logger.info("Crawling already ran successfully for today, skipping run.")


if __name__ == "__main__":
    crawl_voltastics(True)  # type: ignore[call-overload]
