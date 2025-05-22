from prefect.states import State, Failed
from src.common.monitoring.flow import monitored_flow


@monitored_flow
def failing_flow() -> State:
    return Failed(msg="failing_flow fails always as it exists for testing purposes")
