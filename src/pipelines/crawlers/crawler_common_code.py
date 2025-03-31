import asyncio
from asyncio import Queue
from typing import Any, Callable, Coroutine
import time

import prefect
from prefect import task
from prefect.variables import Variable

from src.common.loaders.bq_loader import stream_to_bigquery, is_streaming_buffer_empty
from src.pipelines.crawlers.crawler_config import CrawlerConfig
from src.pipelines.crawlers.crawler_state import CrawlerState

logger = prefect.logging.get_logger()


def _get_state(config: CrawlerConfig) -> CrawlerState:
    return CrawlerState.load_from_variable(config.state_variable_name)


def is_cleanup_needed(config: CrawlerConfig) -> bool:
    state = _get_state(config)
    return state.last_run_day == config.partition_date


def is_production_environment() -> bool:
    return Variable.get("prefect_environment") == "production"


def has_run_successfully_for_day(config: CrawlerConfig) -> bool:
    state = _get_state(config)
    return state.last_successful_day == config.partition_date


async def source_to_sink(
    source: Callable[[Queue[Any]], Coroutine[Any, Any, int]],
    sink: Callable[[Queue[Any]], Coroutine[Any, Any, None]],
) -> int:
    # documents are transported from the source to the sink via this queue
    queue: Queue[Any] = asyncio.Queue()
    async with asyncio.TaskGroup() as group:
        source_task = group.create_task(source(queue))
        group.create_task(sink(queue))
    return source_task.result()


def bigquery_sink(
    config: CrawlerConfig, stop_document: Any = None
) -> Callable[[Queue[Any]], Coroutine[Any, Any, None]]:
    @task
    async def write_to_bigquery(queue: Queue[Any]) -> None:
        async def get_document_from_queue() -> Any:
            result = await queue.get()
            queue.task_done()
            return result

        await stream_to_bigquery(
            *config.get_bq_table_fqdn(), get_document=get_document_from_queue, stop_document=stop_document
        )

    return write_to_bigquery


@task
def update_last_run_day(config: CrawlerConfig) -> None:
    state = _get_state(config)
    state.last_run_day = config.partition_date
    state.persist_to_variable()


@task
def update_last_successful_day(config: CrawlerConfig) -> None:
    state = _get_state(config)
    state.last_successful_day = config.partition_date
    state.persist_to_variable()


@task(timeout_seconds=60 * 90)
def wait_for_bigquery_data_to_be_accessible(config: CrawlerConfig) -> None:
    while True:
        if is_streaming_buffer_empty(*config.get_bq_table_fqdn()):
            break
        time.sleep(60)
