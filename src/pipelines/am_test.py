from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
import aiohttp
import asyncio

from src.pipelines.crawlers.aktion_mensch.pipeline import get_auth_headers

logger = PipelineLogger(__name__)


@monitored_flow
def am_test() -> None:
    async def async_fetch():  # type: ignore
        async with aiohttp.ClientSession() as session:
            # Get the IP information from httpbin
            async with session.get("https://echo-http-requests.appspot.com/echo") as ip_resp:
                ip = await ip_resp.text()

            headers = get_auth_headers()
            try:
                async with session.get(
                    "https://service.test-am.de/api/holi-social/v1/freiwilligendatenbank/export/angebote?page=1&limit=100",
                    headers=headers,
                ) as resp:
                    body = await resp.text()
                    if resp.status != 200:
                        return f"ip: {ip}, HTTP Error: {resp.status}, {body}", resp.status
                    else:
                        return f"ip: {ip}, {body[:400]}", 200
            except aiohttp.ClientError as e:
                return f"ip: {ip}, URL Error: {str(e)}", 500

    result_text, status = asyncio.run(async_fetch())  # type: ignore
    logger.info(f"result_text={result_text}, status={status}")


if __name__ == "__main__":
    am_test()
