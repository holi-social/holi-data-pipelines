from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
import aiohttp
import asyncio
import socket

from src.pipelines.crawlers.aktion_mensch.pipeline import get_auth_headers

logger = PipelineLogger(__name__)


class IPv4OnlyConnector(aiohttp.TCPConnector):
    """Connector that forces IPv4-only connections for outbound requests"""

    async def _resolve_host(self, host, port, traces=None):  # type: ignore
        logger.info(f"Resolving {host}:{port} (IPv4 only)")
        try:
            # Force IPv4 resolution
            infos = await asyncio.get_event_loop().getaddrinfo(
                host,
                port,
                family=socket.AF_INET,  # IPv4 only
                type=socket.SOCK_STREAM,
            )

            resolved = [
                {
                    "hostname": host,
                    "host": info[4][0],
                    "port": info[4][1],
                    "family": info[0],
                    "proto": info[2],
                    "flags": info[3],
                }
                for info in infos
            ]

            logger.info(f"Resolved {host} to IPv4: {[r['host'] for r in resolved]}")
            return resolved

        except Exception as e:
            logger.error(f"Failed to resolve {host}: {e}")
            raise e


# Global HTTP client session with IPv4-only connector
_http_session = None


async def get_http_session() -> aiohttp.ClientSession:  # type: ignore
    """Get or create the global HTTP session with IPv4-only connector"""
    global _http_session
    if _http_session is None or _http_session.closed:
        connector = IPv4OnlyConnector(limit=100, limit_per_host=30, ttl_dns_cache=300, use_dns_cache=True)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        _http_session = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _http_session


@monitored_flow
def am_test() -> None:
    async def async_fetch():  # type: ignore
        async with await get_http_session() as session:
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
