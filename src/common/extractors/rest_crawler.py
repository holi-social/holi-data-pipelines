import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Optional, Awaitable, Iterable
from urllib.parse import urlencode
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

import prefect
from aiohttp import ClientSession, ClientTimeout
from aiohttp_retry import RetryClient, RetryOptionsBase, ExponentialRetry
from codetiming import Timer

logger = prefect.logging.get_logger()

_MAX_RETRY_ATTEMPTS = 3


def _always_succeeding_page_check(page: Any) -> None:
    return


@dataclass
class PagingInfo:
    def __init__(self, offset: int, size: int):
        if offset < 0:
            raise ValueError("Offset must be an integer >= 0")
        if size <= 0:
            raise ValueError("Size must be an integer > 0")
        if offset % size != 0:
            raise ValueError("Offset must be a multiple of the page size")
        self.offset = offset
        self.size = size

    def page_number(self) -> int:
        return (self.offset // self.size) + 1


class RestCrawler:
    _default_retry = ExponentialRetry()
    _default_timeout = ClientTimeout(total=300, sock_connect=10)

    def __init__(
        self,
        url: str,
        get_params: Callable[[PagingInfo], dict[str, Any]],
        get_full_count: Callable[[Any], int] | int,
        skip_auto_headers: Optional[Iterable[str]],
        process_page: Callable[[Any], Awaitable[None]],
        check_if_page_response_is_valid: Callable[[Any], None] = _always_succeeding_page_check,
        page_size: int = 50,
        headers: Optional[dict[str, str]] = None,
        parallelism: int = 1,
        stop_page: Any = None,
        retry_options: Optional[RetryOptionsBase] = _default_retry,
        timeout: Optional[ClientTimeout] = _default_timeout,
        log_function: Callable[[str], None] = logger.info,
    ):
        self.url = url
        self.get_params = get_params
        self.get_full_count = get_full_count
        self.skip_auto_headers = skip_auto_headers
        self.page_size = page_size
        self.headers = headers
        self.process_page = process_page
        self.check_if_page_response_is_valid = check_if_page_response_is_valid
        self.stop_page = stop_page
        # limit amount of parallel requests from the crawler
        self.semaphore = asyncio.Semaphore(parallelism)
        self.retry_options = retry_options
        self.timeout = timeout
        self.log_fn = log_function

    @retry(stop=stop_after_attempt(max_attempt_number=_MAX_RETRY_ATTEMPTS), wait=wait_exponential_jitter())
    async def _fetch_total_entries(self, retry_client: RetryClient) -> int:
        if isinstance(self.get_full_count, int):
            return self.get_full_count
        else:
            async with retry_client.get(
                self.url,
                headers=self.headers,
                params=self.get_params(PagingInfo(0, 1)),
            ) as response:
                data = await response.json()
                return self.get_full_count(data)

    @retry(stop=stop_after_attempt(max_attempt_number=_MAX_RETRY_ATTEMPTS), wait=wait_exponential_jitter())
    async def _fetch_documents(self, retry_client: RetryClient, paging_info: PagingInfo) -> list[Any]:
        params = self.get_params(paging_info)
        url_encoded_params = urlencode(params)
        with Timer(
            text=(
                f"Fetching page #{paging_info.page_number()} ({self.url}{'?' + url_encoded_params if params else ''}) "
                f"took {{:0.4f}} seconds"
            ),
            logger=self.log_fn,
        ):
            async with retry_client.get(
                self.url,
                headers=self.headers,
                params=params,
            ) as response:
                data = await response.json()
                self.check_if_page_response_is_valid(data)
                return data

    async def _fetch_and_process_chunk(self, session: RetryClient, paging_info: PagingInfo) -> None:
        async with self.semaphore:
            page = await self._fetch_documents(session, paging_info)
            await self.process_page(page)

    async def crawl(self) -> None:
        async with ClientSession(timeout=self.timeout, skip_auto_headers=self.skip_auto_headers) as session:
            retry_client = RetryClient(client_session=session, retry_options=self.retry_options)
            number_of_total_entries = await self._fetch_total_entries(retry_client)
            if number_of_total_entries > self.page_size:  # keep quiet if only 1 page will be fetched
                self.log_fn(f"Starting to crawl '{self.url}', {number_of_total_entries} documents available to crawl")
            offset = 0
            async with asyncio.TaskGroup() as group:
                while offset < number_of_total_entries:
                    # Submit a task to fetch the chunk asynchronously
                    group.create_task(self._fetch_and_process_chunk(retry_client, PagingInfo(offset, self.page_size)))
                    offset += self.page_size
                if offset // self.page_size > 1:  # keep quiet if only 1 page will be fetched
                    self.log_fn(f"Scheduled {offset // self.page_size} tasks.")

            await self.process_page(self.stop_page)  # stop signal
