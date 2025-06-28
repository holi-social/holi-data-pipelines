from prefect import flow
from prefect.states import State, Failed
from src.common.monitoring.flow import (
    on_cancellation_hooks,
    on_crashed_hooks,
    on_failure_hooks,
    on_completion_hooks,
)


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def failing_flow() -> State:
    return Failed(msg="failing_flow fails always as it exists for testing purposes")


if __name__ == "__main__":
    failing_flow()
