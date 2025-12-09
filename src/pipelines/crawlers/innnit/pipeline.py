import functools
import json
import asyncio
from typing import Any

from aiohttp import ClientSession
from prefect import flow

from src.common.loaders.bq_loader import (
    delete_partition,
    is_streaming_buffer_empty,
    wait_for_bigquery_data_to_be_accessible,
)
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import (
    on_cancellation_hooks,
    on_completion_hooks,
    on_crashed_hooks,
    on_failure_hooks,
)
from src.common.monitoring.metrics import report_number_of_processed_entities
from src.common.variables import secret_value, variable_value_as
from src.pipelines.crawlers.crawler_common_code import (
    bigquery_sink,
    has_run_successfully_for_day,
    is_cleanup_needed,
    source_to_sink,
    update_last_run_day,
    update_last_successful_day,
)
from src.pipelines.crawlers.crawler_config import CrawlerConfig
from src.pipelines.crawlers.innnit.types import Petition, PetitionRow, _UnhashedPetitionRow, hash_row

logger = PipelineLogger(__name__)
_STOP_OBJ = object()


@functools.cache
def config() -> CrawlerConfig:
    return CrawlerConfig(
        state_variable_name="innnit_crawler_state",
        api_url=variable_value_as(str, "innnit_api_url"),
        bigquery_table=variable_value_as(dict[str, str], "innnit_crawler_table"),
    )


async def crawl(queue: asyncio.Queue[Any]) -> int:
    async def fetch_data() -> list[Petition]:
        api_key = secret_value("innnit-api-key")
        if not api_key:
            raise ValueError("Missing innn.it API key")

        headers = {
            "x-workflow-api-key": api_key,
        }

        async with ClientSession(headers=headers) as session:
            async with session.get(config().api_url) as response:
                return await response.json()

    def petition_to_bq_row(petition: Petition) -> PetitionRow:
        row = _UnhashedPetitionRow(
            id=petition["_id"],
            title=petition["title"],
            slug=petition["name"],
            description=petition["content"],
            signatures=petition["signatureCount"],
            petitionSuccessful=petition["success"],
            tag=petition["tag_title"],
            initiators=json.dumps(petition["initiatorNames"]),
            targets=json.dumps(petition["targetNames"]),
            featuredImage=petition["featuredImage"],
        )
        hash = hash_row(row)

        return PetitionRow(**row, hash=hash)

    async def add_petition_to_queue(petition: Petition) -> None:
        queue.put_nowait(petition_to_bq_row(petition))

    data = await fetch_data()
    for petition in data:
        await add_petition_to_queue(petition)

    queue.put_nowait(_STOP_OBJ)
    return len(data)


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def crawl_innnit(force_run: bool = False, wait_for_bq: bool = True) -> None:
    if has_run_successfully_for_day(config()) and not force_run:
        logger.info("Skipping crawler - already ran successfully today")
        return

    logger.info("Starting innn.it crawler run")
    table_name = f"{config().get_bq_project()}.{config().get_bq_dataset()}.{config().get_bq_table()}"

    if is_cleanup_needed(config()):
        if not is_streaming_buffer_empty(table_name) and not force_run:
            logger.error("Streaming buffer is not empty. Aborting flow.")
            return

        logger.info(f"Cleaning up interrupted run from {config().partition_date}")
        delete_partition(table_name, partition=config().partition_date)

    update_last_run_day(config())

    processed_count = asyncio.run(source_to_sink(crawl, bigquery_sink(config(), _STOP_OBJ)))

    update_last_successful_day(config())
    report_number_of_processed_entities(processed_count)
    logger.info(f"Successfully processed {processed_count} innn.it opportunities")

    if wait_for_bq:
        wait_for_bigquery_data_to_be_accessible(table_name)


if __name__ == "__main__":
    crawl_innnit(force_run=True, wait_for_bq=False)
