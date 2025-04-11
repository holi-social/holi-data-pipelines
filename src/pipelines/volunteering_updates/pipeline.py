from prefect.task_runners import ConcurrentTaskRunner

from src.common.monitoring.flow import monitored_flow
from src.pipelines.crawlers.voltastics.pipeline import crawl_voltastics
from src.pipelines.volunteering_classification.volunteering_classification import volunteering_classification
from src.pipelines.volunteering_geo_enrichment.volunteering_geo_enrichment import volunteering_geo_enrichment
from src.pipelines.volunteering_to_typesense.ingest_updates import volunteering_to_typesense_ingest_updates_flow


@monitored_flow(task_runner=ConcurrentTaskRunner())  # type: ignore
def volunteering_updates() -> None:
    """
    Automation graph to keep volunteering data up-to-date.
    Crawls data sources, classifies and enriches data and updates the Typesense index.
    """
    crawl_result = crawl_voltastics()  # type: ignore

    classification_result = volunteering_classification(wait_for=[crawl_result])  # type: ignore
    geo_enrichment_result = volunteering_geo_enrichment(wait_for=[crawl_result])  # type: ignore

    return volunteering_to_typesense_ingest_updates_flow(wait_for=[classification_result, geo_enrichment_result])  # type: ignore


if __name__ == "__main__":
    volunteering_updates()
