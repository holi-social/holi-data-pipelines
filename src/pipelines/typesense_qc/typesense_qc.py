import pprint
from typing import cast
from urllib.parse import quote

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

DEVIATION_TOLERANCE_FACTOR = 0
DEVIATION_TOLERANCE_IN_PERCENT = 0


@task
def get_profile_ids_bq() -> list[str]:
    result = extract_data_from_bigquery(f"""
        SELECT username
        FROM `{bigquery_okuna_dataset()}.openbook_auth_user` user
        LEFT OUTER JOIN `{bigquery_okuna_dataset()}.openbook_auth_userprofile` profile
        ON user.id=profile.user_id
        WHERE
          user.is_deleted=false
          AND profile IS NOT NULL
          AND user.identity IS NOT NULL
          AND (profile.name IS NOT NULL OR profile.last_name IS NOT NULL) -- users without first or last name shall not be synced to Typesense 
    """)  # noqa: S608
    return cast(list[str], [row["username"] for row in result])


@task
def get_space_ids_bq() -> list[str]:
    result = extract_data_from_bigquery(f"""
        SELECT id
        FROM `{bigquery_okuna_dataset()}.openbook_communities_community`
        WHERE is_deleted=false
    """)  # noqa: S608
    return cast(list[str], [row["id"] for row in result])


@task
def get_task_ids_bq() -> list[str]:
    result = extract_data_from_bigquery(f"""
        SELECT id
        FROM `{tasks_with_classifications()}`
    """)  # noqa: S608
    return cast(list[str], [row["id"] for row in result])


@task
def get_volunteering_ids_bq() -> list[str]:
    result = extract_data_from_bigquery(f"""
        SELECT id
        FROM `{volunteering_voltastics_classified_enriched_partitioned()}`
        WHERE TIMESTAMP_TRUNC(partitiontime, DAY) = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
    """)  # noqa: S608
    return cast(list[str], [row["id"] for row in result])


@task
def get_ids_ts(ts_client: TypesenseClient, ts_collection: str, document_type: str) -> list[str]:
    def strip_prefix(id: str) -> str:
        return id.split(f"{document_type}_")[1]

    url_encoded_ts_collection = quote(ts_collection)

    all_results = []
    page = 1
    per_page = 250
    has_more = True

    while has_more:
        result = ts_client.client.collections[url_encoded_ts_collection].documents.search(
            {
                "q": "*",
                "filter_by": f"type:={document_type}",
                "page": page,
                "per_page": per_page,
                "include_fields": "id",
            }
        )

        page_results = [strip_prefix(cast(str, row["document"]["id"])) for row in result["hits"]]
        all_results.extend(page_results)

        # Check if there are more results to fetch
        if len(page_results) < per_page:
            has_more = False
        else:
            page += 1

    return all_results


@flow
def typesense_qc() -> State:
    ts_client = TypesenseClient(TypesenseClientConfig.from_env())
    ts_collection = cast(str, variable_value("typesense_collection"))

    profile_ids_bq = get_profile_ids_bq()
    profile_ids_ts = get_ids_ts(ts_client, ts_collection, "profile")

    space_ids_bq = get_space_ids_bq()
    space_ids_ts = get_ids_ts(ts_client, ts_collection, "space")

    task_ids_bq = get_task_ids_bq()
    task_ids_ts = get_ids_ts(ts_client, ts_collection, "task")

    volunteering_ids_bq = get_volunteering_ids_bq()
    volunteering_ids_ts = get_ids_ts(ts_client, ts_collection, "volunteering")

    results = {
        "profile": {
            "bq_count": len(profile_ids_bq),
            "ts_count": len(profile_ids_ts),
            "bq_ids_not_in_ts": set(profile_ids_bq) - set(profile_ids_ts),
            "ts_ids_not_in_bq": set(profile_ids_ts) - set(profile_ids_bq),
        },
        "space": {
            "bq_count": len(space_ids_bq),
            "ts_count": len(space_ids_ts),
            "bq_ids_not_in_ts": set(space_ids_bq) - set(space_ids_ts),
            "ts_ids_not_in_bq": set(space_ids_ts) - set(space_ids_bq),
        },
        "task": {
            "bq_count": len(task_ids_bq),
            "ts_count": len(task_ids_bq),
            "bq_ids_not_in_ts": set(task_ids_bq) - set(task_ids_ts),
            "ts_ids_not_in_bq": set(task_ids_ts) - set(task_ids_bq),
        },
        "volunteering": {
            "bq_count": len(volunteering_ids_bq),
            "ts_count": len(volunteering_ids_ts),
            "bq_ids_not_in_ts": set(volunteering_ids_bq) - set(volunteering_ids_ts),
            "ts_ids_not_in_bq": set(volunteering_ids_ts) - set(volunteering_ids_bq),
        },
    }

    all_types = ["profile", "space", "task", "volunteering"]
    errors = []

    for typ in all_types:
        if results[typ]["bq_ids_not_in_ts"] or results[typ]["ts_ids_not_in_bq"]:
            msg = f"BigQuery and Typesense are inconsistent for document type {typ}%: {pprint.pformat(results[typ])}"
            logger.error(msg)
            errors.append(msg)

    if errors:
        return Failed(message="\n".join(errors))

    logger.info(f"Counts are within tolerance range of {DEVIATION_TOLERANCE_IN_PERCENT}% deviation: {results}")

    return Completed()


if __name__ == "__main__":
    typesense_qc()
