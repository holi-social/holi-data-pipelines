import prefect
from src.common.monitoring.flow import monitored_flow
import urllib3

from src.pipelines.crawlers.aktion_mensch.pipeline import get_auth_headers

logger = prefect.logging.get_logger()


@monitored_flow
def am_verify_flow() -> None:
    headers = get_auth_headers()
    with urllib3.PoolManager() as http:
        response = http.request(
            "GET",
            "https://service.test-am.de/api/holi-social/v1/freiwilligendatenbank/export/angebote?page=1&limit=100",
            headers=headers,
        )
        body = response.read().decode("utf-8")
        logger.info(f"Received body={body}")
