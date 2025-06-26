import math
from prefect.states import State, Failed, Completed

from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from typing import cast
from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.pipelines.bigquery import (
    tasks_with_classifications,
    volunteering_voltastics_classified_enriched_partitioned,
    volunteering_aktion_mensch_classified_enriched_partitioned,
)

logger = PipelineLogger(__name__)

TOLERANCE_FACTOR = 0.01


def count_partitioned(view_name: str) -> int:
    result = extract_data_from_bigquery(f"""
        SELECT COUNT(*) as count FROM `{view_name}`
        WHERE TIMESTAMP_TRUNC(partitiontime, DAY) = (SELECT MAX(TIMESTAMP_TRUNC(partitiontime, DAY))
        FROM `{view_name}`)
    """)  # noqa: S608
    return cast(int, result[0]["count"])


def count_tasks() -> int:
    result = extract_data_from_bigquery(f"""
        SELECT COUNT(*) as count FROM `{tasks_with_classifications()}`
    """)  # noqa: S608
    return cast(int, result[0]["count"])


def count_aktion_mensch_volunteering() -> int:
    return count_partitioned(volunteering_aktion_mensch_classified_enriched_partitioned())


def count_volunteering() -> int:
    return count_partitioned(volunteering_voltastics_classified_enriched_partitioned())


def count_classification_failed(view_name: str) -> int:
    result = extract_data_from_bigquery(f"""
        SELECT COUNT(classification_failed) as `count`
        FROM `{view_name}`
        WHERE classification_failed IS TRUE
    """)  # noqa: S608
    return cast(int, result[0]["count"])


def count_classification_failed_partitoned(view_name: str) -> int:
    result = extract_data_from_bigquery(f"""
        SELECT COUNT(classification_failed) as `count`
        FROM `{view_name}`
        WHERE TIMESTAMP_TRUNC(partitiontime, DAY) = (SELECT MAX(TIMESTAMP_TRUNC(partitiontime, DAY)) FROM `{view_name}`)
        AND classification_failed IS TRUE
    """)  # noqa: S608
    return cast(int, result[0]["count"])


def count_classification_failed_aktion_mensch_volunteering() -> int:
    return count_classification_failed_partitoned(volunteering_aktion_mensch_classified_enriched_partitioned())


def count_classification_failed_volunteering() -> int:
    return count_classification_failed_partitoned(volunteering_voltastics_classified_enriched_partitioned())


def count_classification_failed_tasks() -> int:
    return count_classification_failed(tasks_with_classifications())


def fails_tolerance(count: int, count_failed: int) -> bool:
    return not math.isclose(count, count - count_failed, rel_tol=TOLERANCE_FACTOR)


@monitored_flow
def classification_qc() -> State:
    aktion_mensch_total = count_aktion_mensch_volunteering()
    aktion_mensch_failed = count_classification_failed_aktion_mensch_volunteering()
    aktion_mensch_qc_failed = fails_tolerance(aktion_mensch_total, aktion_mensch_failed)
    if aktion_mensch_qc_failed:
        logger.error(
            f"aktion_mensch classification qc failed (total={aktion_mensch_total}, failed={aktion_mensch_failed})"
        )
    else:
        logger.info(
            f"aktion_mensch classification qc succeeded (total={aktion_mensch_total}, failed={aktion_mensch_failed})"
        )

    voltastics_total = count_volunteering()
    voltastics_failed = count_classification_failed_volunteering()
    voltastics_qc_failed = fails_tolerance(voltastics_total, voltastics_failed)
    if voltastics_qc_failed:
        logger.error(f"voltastics classification qc failed (total={voltastics_total}, failed={voltastics_failed})")
    else:
        logger.info(f"voltastics classification qc succeeded (total={voltastics_total}, failed={voltastics_failed})")

    tasks_total = count_tasks()
    tasks_failed = count_classification_failed_tasks()
    tasks_qc_failed = fails_tolerance(tasks_total, tasks_failed)
    if tasks_qc_failed:
        logger.error(f"tasks classification qc failed (total={tasks_total}, failed={tasks_failed})")
    else:
        logger.info(f"tasks classification qc succeeded (total={tasks_total}, failed={tasks_failed})")

    if aktion_mensch_qc_failed or voltastics_qc_failed or tasks_qc_failed:
        return Failed()
    else:
        return Completed()


if __name__ == "__main__":
    classification_qc()
