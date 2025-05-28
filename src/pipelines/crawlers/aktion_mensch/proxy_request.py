import asyncio
import json
import os
import subprocess
import tempfile
import time
from typing import Any, Dict, Optional, List
from urllib.parse import urlencode, urlparse
import prefect

logger = prefect.logging.get_logger()

# SSH tunnel process
_SSH_PROCESS: Optional[subprocess.Popen[Any]] = None
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

        auth_cmd = ["gcloud", "auth", "activate-service-account", f"--key-file={key_path}"]
        subprocess.run(auth_cmd, check=True)  # noqa: S603
        os.unlink(key_path)

    ssh_cmd: List[str] = [
        "gcloud",
        "compute",
        "ssh",
        f"--zone={zone}",
        instance,
        "--tunnel-through-iap",
        f"--project={project}",
        "--",
        "-L",
        f"{_LOCAL_PORT}:{_TARGET_HOST}:{_TARGET_PORT}",
        "-N",
    ]

    _SSH_PROCESS = subprocess.Popen(  # noqa: S603
        ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    time.sleep(3)
    return True


def cleanup_ssh_tunnel() -> None:
    """Clean up SSH tunnel."""
    global _SSH_PROCESS
    if _SSH_PROCESS is not None:
        _SSH_PROCESS.terminate()
        _SSH_PROCESS = None


async def proxy_request(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Send request through SSH tunnel."""
    if _SSH_PROCESS is None or _SSH_PROCESS.poll() is not None:
        setup_ssh_tunnel()

    full_url = url if not params else f"{url}?{urlencode(params)}"

    parsed_url = urlparse(full_url)
    path_and_query = parsed_url.path
    if parsed_url.query:
        path_and_query += f"?{parsed_url.query}"

    local_url = f"https://localhost:{_LOCAL_PORT}{path_and_query}"

    cmd = ["curl", "-k", "-s"]

    if headers:
        for key, value in headers.items():
            cmd.extend(["--header", f"{key}: {value}"])

    cmd.extend(["--header", f"Host: {parsed_url.netloc}"])
    cmd.append(local_url)

    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    stdout, _ = await process.communicate()
    stdout_text = stdout.decode("utf-8")

    json_start = stdout_text.find("{")
    if json_start >= 0:
        return json.loads(stdout_text[json_start:])
    raise ValueError("No JSON in response")
