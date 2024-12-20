import asyncio
from typing import Any, Callable, Optional, Tuple

import prefect
from aiohttp import ClientSession

logger = prefect.logging.get_logger()


type PageIndexAndSize = Tuple[int, int]


class RestCrawler:
    def __init__(
        self,
        url: str,
        get_params: Callable[[PageIndexAndSize], dict[str, Any]],
        get_full_count: Callable[[Any], int],
        process_page: Callable[[Any], None],
        page_size: int = 50,
        headers: Optional[dict[str, str]] = None,
        parallelism: int = 1,
        stop_page: Any = None,
    ):
        self.url = url
        self.get_params = get_params
        self.get_full_count = get_full_count
        self.page_size = page_size
        self.headers = headers
        self.process_page = process_page
        self.stop_page = stop_page
        # limit amount of parallel requests from the crawler
        self.semaphore1 = asyncio.Semaphore(parallelism)
        self.semaphore2 = asyncio.Semaphore(parallelism)

    async def _fetch_total_entries(self, session: ClientSession) -> int:
        async with session.get(self.url, headers=self.headers, params=self.get_params((0, 0))) as response:
            data = await response.json()
            return data.get("totalEntries", 0)

    async def _fetch_documents(self, session: ClientSession, page_index_and_size: PageIndexAndSize) -> list[Any]:
        logger.info(f"Fetching page #{page_index_and_size[0]}")
        async with session.get(
            self.url, headers=self.headers, params=self.get_params(page_index_and_size)
        ) as response:
            data = await response.json()
            logger.info(f"Fetched page #{page_index_and_size[0]}")
            return data

    async def _fetch_and_process_chunk(self, session: ClientSession, page_index: int, page_size: int) -> None:
        async with self.semaphore2:
            logger.info(f"Fetching chunk #{page_index}")
            page = await self._fetch_documents(session, (page_index, page_size))
            self.process_page(page)

    async def crawl(self) -> None:
        async with ClientSession(raise_for_status=True) as session:
            number_of_total_entries = await self._fetch_total_entries(session)
            logger.info(f"{number_of_total_entries} documents available to crawl")
            offset = 0
            tasks = []
            while offset < number_of_total_entries:
                async with self.semaphore1:
                    # Submit a task to fetch the chunk asynchronously
                    logger.info(f"Scheduling fetch with offset {offset}")
                    async_task = asyncio.create_task(
                        self._fetch_and_process_chunk(session, int(offset / self.page_size), self.page_size)
                    )
                    tasks.append(async_task)
                    offset += self.page_size

            # Wait for all tasks to complete
            try:
                await asyncio.gather(*tasks)
            except Exception as e:
                logger.exception("Failed to execute all tasks successfully")
                for t in tasks:
                    t.cancel()
                raise e

            self.process_page(self.stop_page)  # stop signal
