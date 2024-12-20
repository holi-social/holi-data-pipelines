from asyncio import Queue
import asyncio
from asyncio import Queue
from typing import Any

import prefect
from prefect.types import StrictVariableValue

from src.common.extractors.rest_crawler import RestCrawler
from src.common.variables import variable_value

logger = prefect.logging.get_logger()

_CONFIG: dict[str, StrictVariableValue] = {
    "api_endpoint_url": str(variable_value("betterplace_api_endpoint_url")),
}


async def crawl_betterplace() -> None:
    # TODO state in prefect variable(s)
    logger.info("Partition does not yet exist, starting crawling")
    # documents are transported from the crawler to the writer via this queue
    queue: Queue[Any] = asyncio.Queue()

    def add_page_to_queue(page: Any) -> None:
        if page is None:  # stop signal, last element
            queue.put_nowait(None)
        for document in page.get("entries"):
            queue.put_nowait(document)

    crawler = RestCrawler(
        str(_CONFIG["api_endpoint_url"]),
        lambda index_and_size: {
            "page": index_and_size[0],
            "per_page": index_and_size[1],
            "order": "id:DESC",
        },
        lambda page: page.get("totalEntries", 0),
        add_page_to_queue,
    )

    crawler_task = asyncio.create_task(crawler.crawl())
    # bigquery_writer_task = asyncio.create_task(stream_to_bigquery(queue))
    await asyncio.gather(crawler_task)
    # await asyncio.gather(bigquery_writer_task)

    logger.info("finished crawling")


if __name__ == "__main__":
    asyncio.run(crawl_betterplace())
