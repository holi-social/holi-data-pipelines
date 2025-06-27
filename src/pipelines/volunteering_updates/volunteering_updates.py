from typing import cast

from prefect.states import State, Failed, Completed
from prefect.task_runners import ConcurrentTaskRunner

from src.common.monitoring.flow import monitored_flow
from src.pipelines.crawlers.aktion_mensch.pipeline import crawl_aktion_mensch
from src.pipelines.crawlers.voltastics.pipeline import crawl_voltastics
from src.pipelines.volunteering_classification.aktion_mensch_classification import aktion_mensch_classification_flow
from src.pipelines.volunteering_classification.voltastics_classification import voltastics_classification_flow
from src.pipelines.volunteering_geo_enrichment.volunteering_geo_enrichment import voltastics_geo_enrichment_flow
from src.pipelines.volunteering_to_typesense.aktion_mensch_to_typesense_ingest_updates import (
    aktion_mensch_to_typesense_ingest_updates_flow,
)
from src.pipelines.volunteering_to_typesense.voltastics_to_typesense_ingest_updates import (
    voltastics_to_typesense_ingest_updates_flow,
)


@monitored_flow(task_runner=ConcurrentTaskRunner())  # type: ignore
def volunteering_updates_flow() -> State:
    """
    Automation graph to keep volunteering data up to date.
    Crawls data sources, classifies and enriches data and updates the Typesense index.
    """
    am_crawl_result = crawl_aktion_mensch()  # type: ignore
    vo_crawl_result = crawl_voltastics()  # type: ignore

    am_classification_result = aktion_mensch_classification_flow(wait_for=[am_crawl_result])  # type: ignore
    vo_classification_result = voltastics_classification_flow(wait_for=[vo_crawl_result])  # type: ignore

    # no need to geo-enrich Aktion Mensch data because almost all rows that have location information also contain
    # latitude / longitude data. checked production data on 2025-06-26. out of ~12.000 VOs, only 9 did have a location
    # but no latitude/longitude. these 9 had zip codes and city names that would not match (e.g., due to typos).

    vo_geo_enrichment_result = voltastics_geo_enrichment_flow(wait_for=[vo_crawl_result])  # type: ignore

    vo_ingestion_result = cast(
        State,
        voltastics_to_typesense_ingest_updates_flow(wait_for=[vo_classification_result, vo_geo_enrichment_result]),  # type: ignore
    )
    am_ingestion_result = cast(
        State,
        aktion_mensch_to_typesense_ingest_updates_flow(wait_for=[am_classification_result]),  # type: ignore
    )

    # vo_ingestion_result and am_ingestion_result are None even though they shouldn't be (maybe related to decorators)
    return (
        Completed()
        if (vo_ingestion_result is None or vo_ingestion_result.is_completed())
        and (am_ingestion_result is None or am_ingestion_result.is_completed())
        else Failed()
    )


if __name__ == "__main__":
    volunteering_updates_flow()
