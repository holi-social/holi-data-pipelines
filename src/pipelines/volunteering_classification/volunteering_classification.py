from prefect.states import State, Completed
from src.common.monitoring.flow import monitored_flow


@monitored_flow
def volunteering_classification() -> State:
    return Completed()
