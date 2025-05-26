from __future__ import annotations

import asyncio
import json
from asyncio import Queue
from typing import Any, Dict, cast, Callable, Coroutine, Optional

import prefect
from prefect import task

from src.common.extractors.rest_crawler import RestCrawler, PagingInfo
from src.common.loaders.bq_loader import (
    is_streaming_buffer_empty,
    delete_partition,
    wait_for_bigquery_data_to_be_accessible,
)
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
from src.pipelines.crawlers.aktion_mensch.curl_request import curl_request

logger = prefect.logging.get_logger()
_CONFIG: Optional[CrawlerConfig] = None
_STOP_DOC = object()  # Signal object for stopping async processes

AM_COLUMNS_TO_JSON_STRING = [
    "durchfuehrendeOrganisation",
    "bild",
    "attribute",
    "kontaktDurchfuehrendeOrganisation",
    "kontaktVermittelndeStelle",
    "einsatzort",
    "geoposition",
    "verortung",
]


def config() -> CrawlerConfig:
    """Get crawler configuration (API URL, BQ table)."""
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = CrawlerConfig(
            state_variable_name="aktion_mensch_crawler_state",
            api_url=cast(str, variable_value_as(str, "aktion_mensch_api_url")),
            bigquery_table=cast(Dict[str, str], variable_value_as(Dict[str, str], "aktion_mensch_crawler_table")),
        )
    return _CONFIG


def get_auth_headers() -> Dict[str, str]:
    """Fetches API credentials from Prefect secret block."""
    credentials_json = secret_value("aktion-mensch-credentials")
    if not credentials_json:
        raise ValueError("Unable to load Aktion Mensch credentials")

    credentials = json.loads(credentials_json)

    return {
        "x-client_id": credentials.get("client_id"),
        "x-client_secret": credentials.get("client_secret"),
    }


class CurlRestCrawler(RestCrawler):
    """RestCrawler that makes direct HTTP requests using curl."""

    async def _fetch_documents(self, retry_client: Any, paging_info: PagingInfo) -> Any:
        """Override to use curl_request for document fetching."""
        return await curl_request(url=self.url, headers=self.headers, params=self.get_params(paging_info))

    async def _fetch_total_entries(self, retry_client: Any) -> int:
        """Override to use curl_request for fetching total entry count."""
        if isinstance(self.get_full_count, int):
            return self.get_full_count
        data = await curl_request(url=self.url, headers=self.headers, params=self.get_params(PagingInfo(0, 1)))
        return self.get_full_count(data)


@task
def crawl() -> Callable[[Queue[Any]], Coroutine[Any, Any, int]]:
    """Returns an async function that crawls the API and puts processed items onto a queue."""

    def paging_params(paging_info: PagingInfo) -> dict[str, Any]:
        return {
            "page": paging_info.page_number(),
            "limit": 100,
        }

    async def crawl_to_queue(queue: Queue[Any]) -> int:
        """Fetch all data from API, prepare items for BQ, and add to queue."""
        document_count = 0

        async def add_page_to_queue(page: Any) -> None:
            """Process API page data, convert fields, and queue valid documents."""
            nonlocal document_count

            if page is _STOP_DOC:
                queue.put_nowait(_STOP_DOC)
                return

            if not isinstance(page, dict) or "data" not in page or not isinstance(page["data"], list):
                logger.warning(f"Unexpected page structure: {type(page)}")
                return

            for document in page["data"]:
                if isinstance(document, dict):
                    processed_document = document.copy()
                    # Convert specific fields to JSON strings for BQ compatibility
                    for col in AM_COLUMNS_TO_JSON_STRING:
                        if col in processed_document and processed_document[col] is not None:
                            processed_document[col] = json.dumps(processed_document[col])
                    queue.put_nowait(processed_document)
                    document_count += 1

        await CurlRestCrawler(
            url=config().api_url,
            get_params=paging_params,
            get_full_count=lambda page: page.get("meta", {}).get("pagination", {}).get("total", 0),
            process_page=add_page_to_queue,
            headers=get_auth_headers(),
            page_size=100,
            stop_page=_STOP_DOC,
        ).crawl()

        return document_count

    return crawl_to_queue


@monitored_flow
def crawl_aktion_mensch(force_run: bool = False, wait_for_bq: bool = True) -> None:
    """
    Main flow: crawl Aktion Mensch API and store results in BigQuery.

    Checks if run already completed for the day unless `force_run` is True.

    Args:
        force_run: Run even if already completed today.
        wait_for_bq: Wait for data to be queryable after load.
    """
    if not has_run_successfully_for_day(config()) or force_run:
        logger.info("Starting Aktion Mensch crawler run")
        table_name = f"{config().get_bq_project()}.{config().get_bq_dataset()}.{config().get_bq_table()}"

        if is_cleanup_needed(config()):
            if is_streaming_buffer_empty(table_name) or force_run:
                logger.info(f"Cleaning up interrupted run from {config().partition_date}")
                delete_partition(table_name, partition=config().partition_date)
            else:
                logger.warning("Streaming buffer not empty. Aborting flow.")
                return

        update_last_run_day(config())
        processed_count = asyncio.run(source_to_sink(crawl(), bigquery_sink(config(), _STOP_DOC)))
        update_last_successful_day(config())
        report_number_of_processed_entities(processed_count)
        logger.info(f"Successfully processed {processed_count} Aktion Mensch opportunities")

        if wait_for_bq:
            wait_for_bigquery_data_to_be_accessible(table_name)
    else:
        logger.info("Skipping crawler - already ran successfully today")


if __name__ == "__main__":
    crawl_aktion_mensch(force_run=True, wait_for_bq=False)  # type: ignore
