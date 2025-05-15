import asyncio
import json
import functools
from typing import Any, Dict, Callable
from urllib.parse import urlencode

import prefect

from src.common.extractors.rest_crawler import PagingInfo

logger = prefect.logging.get_logger()

async def proxy_request(
    url: str, 
    headers: Dict[str, str] = None,
    params: Dict[str, Any] = None,
    zone: str = "europe-north1-a",
    instance: str = "prefect-server-staging-green", 
    project: str = "holi-shared"
) -> Dict[str, Any]:
    """Proxy an HTTP request through a compute instance via SSH."""
    
    full_url = url
    if params:
        full_url = f"{url}?{urlencode(params)}"
        
    headers_args = ""
    if headers:
        for key, value in headers.items():
            # Double-escape quotes for the shell command
            headers_args += f' --header \\"{key}: {value}\\"'
            
    curl_cmd = f'curl -s \\"{full_url}\\"{headers_args}'
    
    cmd = (
        f'gcloud compute ssh --zone "{zone}" "{instance}" '
        f'--tunnel-through-iap --project "{project}" --command="{curl_cmd}"'
    )
    
    logger.info(f"Executing proxy request for {url}")
    
    process = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    stdout, stderr = await process.communicate()
    stdout_text = stdout.decode('utf-8')
    stderr_text = stderr.decode('utf-8')
    
    json_start = stdout_text.find('{')
    if json_start == -1:
        if process.returncode != 0:
            raise RuntimeError(f"SSH proxy error (code {process.returncode}): {stderr_text}")
        raise ValueError(f"No JSON found in response: {stdout_text}")
        
    try:
        payload = stdout_text[json_start:]
        return json.loads(payload)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON response: {e}")

def with_proxy_fetch(func: Callable) -> Callable:
    """Decorator to replace a RestCrawler's _fetch_documents method with a proxied version."""
    @functools.wraps(func)
    async def wrapper(self, retry_client, paging_info):
        """Replace the actual HTTP request with a proxy request through SSH."""
        params = self.get_params(paging_info)
        data = await proxy_request(
            url=self.url,
            headers=self.headers,
            params=params
        )
        return data
    return wrapper 


def with_proxy_fetch_total(func: Callable) -> Callable:
    """Decorator to replace a RestCrawler's _fetch_total_entries method with a proxied version."""
    @functools.wraps(func)
    async def wrapper(self, retry_client):
        """Replace the fetch total entries request with a proxy request through SSH."""
        if isinstance(self.get_full_count, int):
            return self.get_full_count
        else:
            params = self.get_params(PagingInfo(0, 1))
            data = await proxy_request(
                url=self.url, 
                headers=self.headers,
                params=params
            )
            return self.get_full_count(data)
    return wrapper