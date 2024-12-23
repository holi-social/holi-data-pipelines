import asyncio
from asyncio import Queue
from typing import Any, Callable, Coroutine

import prefect
from prefect import task

from src.common.loaders.bq_loader import is_streaming_buffer_empty, delete_partition, stream_to_bigquery
from src.pipelines.crawlers.crawler_config import CrawlerConfig
from src.pipelines.crawlers.crawler_state import CrawlerState

logger = prefect.logging.get_logger()


@task
def check_and_setup(config: CrawlerConfig, force: bool) -> bool:
    state = CrawlerState.get(config.state_variable_name)

    if state.last_successful_day == config.partition_date and not force:
        logger.info("Crawling already ran successfully for today, skipping run.")
        return False
    else:
        logger.info("Crawling did not yet run successfully for today, starting run.")

        needs_cleanup = state.last_run_day == config.partition_date
        if needs_cleanup:
            if is_streaming_buffer_empty(*config.get_bq_table_fqdn()):
                logger.info("Unfinished run happened for today, clearing today's partition.")
                delete_partition(*config.get_bq_table_fqdn(), config.partition_date)
            elif not force:
                logger.warning("Unfinished run happened for today, but streaming buffer is not empty. Aborting flow.")
                return False

        state.last_run_day = config.partition_date
        state.persist()
        return True


async def source_to_sink(
    source: Callable[[Queue[Any]], Coroutine[Any, Any, None]], sink: Callable[[Queue[Any]], Coroutine[Any, Any, None]]
) -> None:
    # documents are transported from the source to the sink via this queue
    queue: Queue[Any] = asyncio.Queue()
    async with asyncio.TaskGroup() as group:
        group.create_task(source(queue))
        group.create_task(sink(queue))


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
def finalize(config: CrawlerConfig) -> None:
    state = CrawlerState.get(config.state_variable_name)
    state.last_successful_day = config.partition_date
    state.persist()
