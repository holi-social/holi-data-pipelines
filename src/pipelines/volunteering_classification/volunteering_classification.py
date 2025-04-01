from prefect.states import State, Completed

from src.common.monitoring.flow import monitored_flow
from src.pipelines.volunteering_classification.topics_skills_classification_v2 import (
    compute_topics_skills_classifications_v2,
)


@monitored_flow
def volunteering_classification() -> State:
    compute_topics_skills_classifications_v2()
    return Completed()


if __name__ == "__main__":
    volunteering_classification()
