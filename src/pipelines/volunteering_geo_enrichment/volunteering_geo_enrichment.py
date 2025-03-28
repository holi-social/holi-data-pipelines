from prefect.state import State, Completed
from src.common.monitoring.flow import monitored_flow


@monitored_flow
def volunteering_geo_enrichment() -> State:
    return Completed()
