from prefect.task_runners import ConcurrentTaskRunner

from src.common.monitoring.flow import monitored_flow
from src.pipelines.crawlers.aktion_mensch.pipeline import crawl_aktion_mensch
from src.pipelines.crawlers.voltastics.pipeline import crawl_voltastics
from src.pipelines.volunteering_classification.aktion_mensch_classification import aktion_mensch_classification_flow
from src.pipelines.volunteering_classification.voltastics_classification import voltastics_classification_flow
from src.pipelines.volunteering_geo_enrichment.volunteering_geo_enrichment import voltastics_geo_enrichment_flow
from src.pipelines.volunteering_to_typesense.voltastics_to_typesense_ingest_updates import (
    voltastics_to_typesense_ingest_updates_flow,
)


@monitored_flow(task_runner=ConcurrentTaskRunner())  # type: ignore
def volunteering_updates_flow() -> None:
    """
    Automation graph to keep volunteering data up-to-date.
    Crawls data sources, classifies and enriches data and updates the Typesense index.
    """
    am_crawl_result = crawl_aktion_mensch()  # type: ignore
    vo_crawl_result = crawl_voltastics()  # type: ignore

    am_classification_result = aktion_mensch_classification_flow(wait_for=[am_crawl_result])  # type: ignore
    vo_classification_result = voltastics_classification_flow(wait_for=[vo_crawl_result])  # type: ignore

    # no need to geo-enrich Aktion Mensch data because almost all rows that have location information also contain
    # latitude / longitude data
    # TODO HOLI-11694 double-check after switching to production environment

    vo_geo_enrichment_result = voltastics_geo_enrichment_flow(wait_for=[vo_crawl_result])  # type: ignore

    return voltastics_to_typesense_ingest_updates_flow(
        wait_for=[am_classification_result, vo_classification_result, vo_geo_enrichment_result]
    )  # type: ignore


if __name__ == "__main__":
    volunteering_updates_flow()
