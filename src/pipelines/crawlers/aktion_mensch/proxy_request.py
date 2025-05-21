import asyncio
import json
import functools
import subprocess
import tempfile
import os
import time
from typing import Any, Dict, Callable, Optional
from urllib.parse import urlencode, urlparse
import prefect
from src.common.extractors.rest_crawler import PagingInfo

logger = prefect.logging.get_logger()

# SSH tunnel process
_SSH_PROCESS = None
_LOCAL_PORT = 8080
_TARGET_HOST = "service.test-am.de"
_TARGET_PORT = 443


def setup_ssh_tunnel(
    zone: str = "europe-north1-a",
    instance: str = "prefect-server-staging-green",
    project: str = "holi-shared",
    service_account_key: Optional[str] = None,
) -> bool:
    """Set up SSH tunnel with port forwarding."""
    global _SSH_PROCESS
    
    if _SSH_PROCESS is not None and _SSH_PROCESS.poll() is None:
        return True
        
    if service_account_key:
        fd, key_path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w") as f:
            f.write(service_account_key)
        subprocess.run(f"gcloud auth activate-service-account --key-file={key_path}", shell=True)
        os.unlink(key_path)
        
    cmd = f'gcloud compute ssh --zone "{zone}" "{instance}" --tunnel-through-iap --project "{project}" -- -L {_LOCAL_PORT}:{_TARGET_HOST}:{_TARGET_PORT} -N'
    _SSH_PROCESS = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(3)
    return True


def cleanup_ssh_tunnel():
    """Clean up SSH tunnel."""
    global _SSH_PROCESS
    if _SSH_PROCESS is not None:
        _SSH_PROCESS.terminate()
        _SSH_PROCESS = None


async def proxy_request(
    url: str,
    headers: Dict[str, str] = None,
    params: Dict[str, Any] = None,
    zone: str = "europe-north1-a",
    instance: str = "prefect-server-staging-green",
    project: str = "holi-shared",
) -> Dict[str, Any]:
    """Send request through SSH tunnel."""
    if _SSH_PROCESS is None or _SSH_PROCESS.poll() is not None:
        # Tunnel not running, try to restart it
        setup_ssh_tunnel()
    
    full_url = url if not params else f"{url}?{urlencode(params)}"
    
    # Parse URL and create local proxy URL
    parsed_url = urlparse(full_url)
    path_and_query = parsed_url.path
    if parsed_url.query:
        path_and_query += f"?{parsed_url.query}"
        
    local_url = f"https://localhost:{_LOCAL_PORT}{path_and_query}"
    
    # Build curl command
    cmd = ["curl", "-k", "-s"]
    
    if headers:
        for key, value in headers.items():
            cmd.extend(["--header", f"{key}: {value}"])
    
    # Add Host header
    cmd.extend(["--header", f"Host: {parsed_url.netloc}"])
    cmd.append(local_url)
    
    # Execute curl through tunnel
    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    
    stdout, _ = await process.communicate()
    stdout_text = stdout.decode("utf-8")
    
    # Find and parse JSON
    json_start = stdout_text.find("{")
    if json_start >= 0:
        return json.loads(stdout_text[json_start:])
    raise ValueError("No JSON in response")


def with_proxy_fetch(func: Callable) -> Callable:
    """Replace RestCrawler's _fetch_documents with proxy version."""
    @functools.wraps(func)
    async def wrapper(self, retry_client, paging_info):
        return await proxy_request(url=self.url, headers=self.headers, params=self.get_params(paging_info))
    return wrapper


def with_proxy_fetch_total(func: Callable) -> Callable:
    """Replace RestCrawler's _fetch_total_entries with proxy version."""
    @functools.wraps(func)
    async def wrapper(self, retry_client):
        if isinstance(self.get_full_count, int):
            return self.get_full_count
        data = await proxy_request(url=self.url, headers=self.headers, params=self.get_params(PagingInfo(0, 1)))
        return self.get_full_count(data)
    return wrapper
