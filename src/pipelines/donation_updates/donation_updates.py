from prefect import flow
from prefect.states import State, Failed
from src.pipelines.crawlers.betterplace.pipeline import crawl_betterplace
from src.pipelines.donation_events.donation_events import donation_events_flow
from src.common.monitoring.flow import (
    on_cancellation_hooks,
    on_crashed_hooks,
    on_failure_hooks,
    on_completion_hooks,
)
from src.common.logging.PipelineLogger import PipelineLogger

logger = PipelineLogger(__name__)


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def donation_updates_flow() -> State:
    """
    Automation graph to emit donation updated, created and deleted events to pubsub.
    """
    crawler_result = crawl_betterplace()
    if not crawler_result:
        return Failed(message="Crawler run was not successful")

    (old_partition_date, new_partition_date) = crawler_result

    logger.info(f"Old partition date: {old_partition_date}, new partition date: {new_partition_date}")

    return donation_events_flow(old_partition_date, new_partition_date)


if __name__ == "__main__":
    donation_updates_flow()
