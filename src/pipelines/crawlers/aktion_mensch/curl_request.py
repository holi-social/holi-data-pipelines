import asyncio
import json
from typing import Any, Dict, Optional
from urllib.parse import urlencode


async def curl_request(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Send HTTP request using curl."""
    if params:
        url = f"{url}?{urlencode(params)}"

    cmd = ["curl", "-s"]

    if headers:
        for key, value in headers.items():
            cmd.extend(["-H", f"{key}: {value}"])

    cmd.append(url)

    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        raise RuntimeError(f"Curl failed: {stderr.decode()}")

    return json.loads(stdout.decode())
