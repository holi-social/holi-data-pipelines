from prefect import flow, State, Completed, Failed
from typing import cast

from src.common.monitoring.flow import on_cancellation_hooks, on_completion_hooks, on_crashed_hooks, on_failure_hooks
from src.pipelines.crawlers.innnit.pipeline import crawl_innnit
from src.pipelines.petitions_to_typesense.innnit_to_typesense_ingest_updates import (
    petitions_innnit_to_typesense_ingest_updates_flow,
)


@flow(
    on_completion=on_completion_hooks,
    on_failure=on_failure_hooks,
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
)
def petitions_updates_flow() -> State:
    innnit_crawl_result = crawl_innnit()  # type: ignore
    innnit_ingestion_result = petitions_innnit_to_typesense_ingest_updates_flow(wait_for=[innnit_crawl_result])  # type: ignore

    innnit_ingestion_result = cast(State, innnit_ingestion_result)

    return Completed() if innnit_ingestion_result is None or innnit_ingestion_result.is_completed() else Failed()


if __name__ == "__main__":
    petitions_updates_flow()
