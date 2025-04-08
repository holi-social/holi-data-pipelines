import json
import math
from typing import cast

from prefect import flow, task
from prefect.states import Completed, Failed, State

from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.common.variables import variable_value
from src.pipelines.bigquery import (
    bigquery_okuna_dataset,
    tasks_with_classifications,
    volunteering_voltastics_classified_enriched_partitioned,
)

logger = PipelineLogger(__name__)

DEVIATION_TOLERANCE_FACTOR = 0.01
DEVIATION_TOLERANCE_IN_PERCENT = 1


@task
def get_profile_count_bq() -> int:
    result = extract_data_from_bigquery(f"""
        SELECT count(*) as user_count
        FROM `{bigquery_okuna_dataset}.openbook_auth_user` user
        LEFT OUTER JOIN `{bigquery_okuna_dataset}.openbook_auth_userprofile` profile
        ON user.id=profile.user_id
        WHERE
          user.is_deleted=false
          AND profile IS NOT NULL
          AND user.identity IS NOT NULL
    """)  # noqa: S608
    return cast(int, result[0]["user_count"])


@task
def get_space_count_bq() -> int:
    result = extract_data_from_bigquery(f"""
        SELECT count(*) as space_count
        FROM `{bigquery_okuna_dataset}.openbook_communities_community`
        WHERE is_deleted=false
    """)  # noqa: S608
    return cast(int, result[0]["space_count"])


@task
def get_task_count_bq() -> int:
    result = extract_data_from_bigquery(f"""
        SELECT count(*) AS task_count
        FROM `{tasks_with_classifications}`
    """)  # noqa: S608
    return cast(int, result[0]["task_count"])


@task
def get_volunteering_count_bq() -> int:
    result = extract_data_from_bigquery(f"""
        SELECT count(*) as volunteering_count
        FROM `{volunteering_voltastics_classified_enriched_partitioned}`
        WHERE TIMESTAMP_TRUNC(partitiontime, DAY) = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
    """)  # noqa: S608
    return cast(int, result[0]["volunteering_count"])


@task
def get_count_ts(ts_client: TypesenseClient, ts_collection: str, document_type: str) -> int:
    result = ts_client.client.collections[ts_collection].documents.search(
        {
            "q": "*",
            "filter_by": f"type:={document_type}",
            "page": 1,
            "per_page": 1,
        }
    )
    return result["found"]


@flow
def typesense_qc() -> State:
    ts_client = TypesenseClient(TypesenseClientConfig.from_env())
    ts_collection = cast(str, variable_value("typesense_collection"))

    profile_count_bq = get_profile_count_bq()
    profile_count_ts = get_count_ts(ts_client, ts_collection, "profile")
    profile_count_deviates = not math.isclose(profile_count_bq, profile_count_ts, rel_tol=DEVIATION_TOLERANCE_FACTOR)

    space_count_bq = get_space_count_bq()
    space_count_ts = get_count_ts(ts_client, ts_collection, "space")
    space_count_deviates = not math.isclose(space_count_bq, space_count_ts, rel_tol=DEVIATION_TOLERANCE_FACTOR)

    task_count_bq = get_task_count_bq()
    task_count_ts = get_count_ts(ts_client, ts_collection, "task")
    task_count_deviates = not math.isclose(task_count_bq, task_count_ts, rel_tol=DEVIATION_TOLERANCE_FACTOR)

    volunteering_count_bq = get_volunteering_count_bq()
    volunteering_count_ts = get_count_ts(ts_client, ts_collection, "volunteering")
    volunteering_count_deviates = not math.isclose(
        volunteering_count_bq, volunteering_count_ts, rel_tol=DEVIATION_TOLERANCE_FACTOR
    )

    counts = {
        "profile_count_bq": profile_count_bq,
        "profile_count_ts": profile_count_ts,
        "space_count_bq": space_count_bq,
        "space_count_ts": space_count_ts,
        "task_count_bq": task_count_bq,
        "task_count_ts": task_count_ts,
        "volunteering_count_bq": volunteering_count_bq,
        "volunteering_count_ts": volunteering_count_ts,
    }

    if profile_count_deviates or space_count_deviates or task_count_deviates or volunteering_count_deviates:
        message = f"Some counts deviate by more than {DEVIATION_TOLERANCE_IN_PERCENT}%: {json.dumps(counts, indent=4)}"
        logger.error(message)
        return Failed(message=message)

    logger.info(
        f"Counts are within tolerance range of {DEVIATION_TOLERANCE_IN_PERCENT}% deviation: {json.dumps(counts, indent=4)}"
    )

    return Completed()


if __name__ == "__main__":
    typesense_qc()
