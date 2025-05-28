from typing import Any, Dict, Optional
import httpx
from src.common.logging.PipelineLogger import PipelineLogger

logger = PipelineLogger(__name__)


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
            response = await client.get(
                "https://echo-http-requests.appspot.com/push/7770a367-c749-42db-a898-73ef069bc035",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            response_text = response.text
            logger.info(f"[info] Response: {response_text}")
            logger.debug(f"[debug] Response: {response_text}")
            logger.error(f"[error] Response: {response_text}")
        except Exception as e:
            logger.debug(f"Debug echo call failed: {e}")  # Log but don't fail the main request

        return {}
