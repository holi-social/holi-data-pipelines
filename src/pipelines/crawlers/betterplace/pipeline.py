from __future__ import annotations

import asyncio
from asyncio import Queue
from typing import Any, Callable, Coroutine, Dict, Optional, cast
from datetime import date
from aiohttp import ClientSession, ClientTimeout, ClientResponse
from aiohttp_retry import RetryClient, ExponentialRetry
from codetiming import Timer
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

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
    stdout_sink,
)
from src.pipelines.crawlers.crawler_config import CrawlerConfig

logger = prefect.logging.get_logger()
_CONFIG: Optional[CrawlerConfig] = None
_HEADERS: Dict[str, str] = {"X-Requested-With": "holi"}
_STOP_DOC = object()
# Retry on all statuses except 200
_RETRY_STATUSES = {x for x in range(100, 600) if x != 200}
_MAX_RETRY_ATTEMPTS = 5
_TIMEOUT = ClientTimeout(total=600, sock_connect=10)  # 10min


class ResponseAwareExponentialRetry(ExponentialRetry):
    def __init__(self) -> None:
        super().__init__(
            statuses=_RETRY_STATUSES,
            evaluate_response_callback=self._check_status_for_retry,
            attempts=_MAX_RETRY_ATTEMPTS,
        )

    async def _check_status_for_retry(self, response: ClientResponse) -> bool:
        return response.status != 200

    def get_timeout(self, attempt: int, response: Optional[ClientResponse] = None) -> float:
        if response is None or "Retry-After" not in response.headers:
            return super().get_timeout(attempt, response)
        logger.info(f"Got retry timeout header: {response.status} {response.headers.get('Retry-After')}")
        return int(response.headers["Retry-After"])


_RETRY_OPTIONS = ResponseAwareExponentialRetry()


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
    return f"{config().api_url}/projects.json?facets=completed:false|closed:false|prohibit_donations:false"


def categories_url(project_id: int) -> str:
    return f"{config().api_url}/projects/{project_id}/categories.json?page=1&per_page=50"


def project_en_url(project_id: int) -> str:
    return f"https://api.betterplace.org/en/api_v4/projects/{project_id}.json"


def get_url_with_relation(links: Optional[list[Any]], relation: str) -> Optional[str]:
    if not links:
        return None
    matching_links = [link for link in links if link.get("rel") == relation]
    if len(matching_links) == 0:
        return None
    return matching_links[0].get("href")


# @task(cache_policy=TASK_SOURCE + RUN_ID)
@task
def crawl() -> Callable[[Queue[Any]], Coroutine[Any, Any, int]]:
    def projects_paging_params(paging_info: PagingInfo) -> dict[str, Any]:
        return {
            "page": paging_info.page_number(),
            "per_page": paging_info.size,
            "order": "id:DESC",
        }

    def get_full_count(page: Any) -> int:
        if "total_entries" not in page:
            raise Exception("Invalid response no total_entries")
        return page.get("total_entries")

    def check_if_page_response_is_valid(page: Any) -> None:
        if "data" not in page:
            raise Exception("Invalid response no data")

    async def crawl_to_queue(queue: Queue[Any]) -> int:
        document_count = 0

        async def add_page_to_queue(projects_page: Any) -> None:
            nonlocal document_count

            # we need to perform sub requests for fetching more detail information
            @retry(stop=stop_after_attempt(max_attempt_number=_MAX_RETRY_ATTEMPTS), wait=wait_exponential_jitter())
            async def fetch_categories(retry_client: RetryClient, project: Any) -> Any:
                async with retry_client.get(categories_url(project["id"])) as response:
                    response_json = await response.json()
                    categories = response_json.get("data", []) if response_json else []
                    return project | {"categories": categories}

            # pragmatically picking only selected texts in English instead of the whole document as only they are being
            # used in the data pipeline downstream
            @retry(stop=stop_after_attempt(max_attempt_number=_MAX_RETRY_ATTEMPTS), wait=wait_exponential_jitter())
            async def fetch_en_texts(retry_client: RetryClient, project: Any) -> Any:
                async with retry_client.get(project_en_url(project["id"])) as response:
                    response_json = await response.json()
                    if not response_json:
                        return project
                    return project | {
                        "country_en": response_json.get("country"),
                        "summary_en": response_json.get("summary"),
                        "title_en": response_json.get("title"),
                        "description_en": response_json.get("description"),
                        "carrier_country_en": response_json.get("carrier", {}).get("country"),
                    }

            # pragmatically picking only carrier_url instead of the full carrier object as only the URL is being used
            # in the data pipeline downstream
            @retry(stop=stop_after_attempt(max_attempt_number=_MAX_RETRY_ATTEMPTS), wait=wait_exponential_jitter())
            async def fetch_carrier_url(retry_client: RetryClient, project: Any) -> Any:
                carrier_links = project.get("carrier", {}).get("links")
                carrier_details_url = get_url_with_relation(carrier_links, "self")
                if not carrier_details_url:
                    return project
                async with retry_client.get(carrier_details_url, headers=_HEADERS) as response:
                    data = await response.json()
                    if not data:
                        return project
                    carrier_url = get_url_with_relation(data.get("links"), "website") or get_url_with_relation(
                        data.get("links"), "platform"
                    )
                    return project | {"carrier_url": carrier_url}

            if projects_page is _STOP_DOC:  # stop signal, last element
                queue.put_nowait(_STOP_DOC)
            else:
                async with ClientSession(timeout=_TIMEOUT) as session:
                    retry_client = RetryClient(
                        client_session=session,
                        retry_options=_RETRY_OPTIONS,
                    )
                    for project in projects_page.get("data"):
                        document_count += 1
                        with Timer(
                            text=(f"Fetching data for project {project['id']} took {{:0.4f}} seconds"),
                            logger=logger.info,
                        ):
                            project_with_categories = await fetch_categories(retry_client, project)
                            project_with_carrier_url = await fetch_carrier_url(retry_client, project_with_categories)
                            project_with_en_texts = await fetch_en_texts(retry_client, project_with_carrier_url)
                            queue.put_nowait(project_with_en_texts)

        await RestCrawler(
            url=projects_url(),
            get_params=projects_paging_params,
            get_full_count=get_full_count,
            skip_auto_headers=[],
            process_page=add_page_to_queue,
            check_if_page_response_is_valid=check_if_page_response_is_valid,
            headers=_HEADERS,
            parallelism=5,
            page_size=50,
            stop_page=_STOP_DOC,
            retry_options=_RETRY_OPTIONS,
            timeout=_TIMEOUT,
            only_log_exceptions=True,
        ).crawl()
        return document_count

    return crawl_to_queue


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def crawl_betterplace(
    force_run: bool = False, wait_for_bq: bool = True, dry_run: bool = False
) -> Optional[tuple[Optional[date], date]]:
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
        if dry_run:
            processed_entities_count = asyncio.run(source_to_sink(crawl(), stdout_sink(_STOP_DOC)))
        else:
            processed_entities_count = asyncio.run(
                source_to_sink(crawl(), bigquery_sink(config(), stop_document=_STOP_DOC))
            )
            update_last_successful_day(config())  # = finalize
            report_number_of_processed_entities(processed_entities_count)
        if wait_for_bq:
            wait_for_bigquery_data_to_be_accessible(table_name)
        return (old_partition_date, config().partition_date)
    else:
        raise Exception("Crawling already ran successfully for today, skipping run.")


if __name__ == "__main__":
    crawl_betterplace(force_run=True, wait_for_bq=False, dry_run=False)  # type: ignore[call-overload]
