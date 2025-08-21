from __future__ import annotations

import asyncio
import functools
from asyncio import Queue
from typing import Any, Awaitable, Callable, Coroutine, Dict, Optional, cast
from datetime import date

import prefect
from prefect import task, flow

from src.common.extractors.rest_crawler import PagingInfo, RestCrawler
from src.common.loaders.bq_loader import (
    delete_partition,
    is_streaming_buffer_empty,
    wait_for_bigquery_data_to_be_accessible,
)
from src.common.monitoring.flow import (
    on_cancellation_hooks,
    on_crashed_hooks,
    on_failure_hooks,
    on_completion_hooks,
)
from src.common.monitoring.metrics import report_number_of_processed_entities
from src.common.variables import variable_value_as
from src.pipelines.crawlers.crawler_common_code import (
    bigquery_sink,
    get_last_successful_day,
    has_run_successfully_for_day,
    is_cleanup_needed,
    source_to_sink,
    update_last_run_day,
    update_last_successful_day,
)
from src.pipelines.crawlers.crawler_config import CrawlerConfig

logger = prefect.logging.get_logger()
_CONFIG: Optional[CrawlerConfig] = None
_HEADERS: Dict[str, str] = {"X-Requested-With": "holi"}
_STOP_DOC = object()


def config() -> CrawlerConfig:
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = CrawlerConfig(
            state_variable_name="betterplace_crawler_state",
            # FIXME remove casting once https://github.com/PrefectHQ/prefect/issues/16825 has a resolution
            api_url=cast(str, variable_value_as(str, "betterplace_api_url")),
            bigquery_table=cast(Dict[str, str], variable_value_as(Dict[str, str], "betterplace_crawler_table")),
        )
    return _CONFIG


def projects_url() -> str:
    return f"{config().api_url}/projects.json"


def categories_url(project_id: int) -> str:
    return f"{config().api_url}/projects/{project_id}/categories.json"


# @task(cache_policy=TASK_SOURCE + RUN_ID)
@task
def crawl() -> Callable[[Queue[Any]], Coroutine[Any, Any, int]]:
    def projects_paging_params(paging_info: PagingInfo) -> dict[str, Any]:
        return {
            "page": paging_info.page_number(),
            "per_page": paging_info.size,
            "order": "id:DESC",
        }

    async def crawl_to_queue(queue: Queue[Any]) -> int:
        document_count = 0

        async def add_page_to_queue(projects_page: Any) -> None:
            nonlocal document_count

            # we need to perform sub requests for fetching the categories
            async def fetch_categories(project_id: int, process_page: Callable[[Any], Awaitable[None]]) -> None:
                default_page_size = 50
                await RestCrawler(
                    url=categories_url(project_id),
                    get_params=lambda _: {},
                    get_full_count=default_page_size,  # instead of fetching, we just tell the crawler 1 page is enough
                    skip_auto_headers=None,
                    process_page=process_page,
                    page_size=default_page_size,
                    headers=_HEADERS,
                    stop_page=_STOP_DOC,
                    log_function=logger.debug,
                ).crawl()

            async def get_project_with_categories(local_queue: Queue[Any], project: Any, categories_page: Any) -> None:
                if (
                    categories_page is not _STOP_DOC
                ):  # skip stop signal, for categories we always get a single page only
                    project_with_categories = project | {"categories": categories_page.get("data")}
                    local_queue.put_nowait(project_with_categories)

            if projects_page is _STOP_DOC:  # stop signal, last element
                queue.put_nowait(_STOP_DOC)
            else:
                categories_queue: Queue[Any] = asyncio.Queue()
                for document in projects_page.get("data"):
                    document_count += 1
                    await fetch_categories(
                        document["id"],
                        functools.partial(get_project_with_categories, categories_queue, document),
                    )
                    queue.put_nowait(await categories_queue.get())
                    categories_queue.task_done()

        await RestCrawler(
            url=projects_url(),
            get_params=projects_paging_params,
            get_full_count=lambda page: page.get("total_entries", 0),
            skip_auto_headers=[],
            process_page=add_page_to_queue,
            headers=_HEADERS,
            parallelism=10,
            stop_page=_STOP_DOC,
        ).crawl()
        return document_count

    return crawl_to_queue


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def crawl_betterplace(force_run: bool = False, wait_for_bq: bool = True) -> Optional[tuple[Optional[date], date]]:
    old_partition_date = get_last_successful_day(config())

    # only run for days that have not been successfully crawled yet, or when forced
    if not has_run_successfully_for_day(config()) or force_run:
        logger.info("Crawling did not run successfully for today, starting run.")
        table_name = f"{config().get_bq_project()}.{config().get_bq_dataset()}.{config().get_bq_table()}"

        if is_cleanup_needed(config()):
            if is_streaming_buffer_empty(table_name) or force_run:
                logger.info("Unfinished run happened for today, clearing today's partition.")
                delete_partition(table_name, partition=config().partition_date)
            else:
                raise Exception("Unfinished run happened for today, but streaming buffer is not empty. Aborting flow.")

        update_last_run_day(config())
        processed_entities_count = asyncio.run(source_to_sink(crawl(), bigquery_sink(config(), _STOP_DOC)))
        update_last_successful_day(config())  # = finalize
        report_number_of_processed_entities(processed_entities_count)
        if wait_for_bq:
            wait_for_bigquery_data_to_be_accessible(table_name)
        return (old_partition_date, config().partition_date)
    else:
        raise Exception("Crawling already ran successfully for today, skipping run.")


if __name__ == "__main__":
    crawl_betterplace(force_run=True, wait_for_bq=False)  # type: ignore[call-overload]
