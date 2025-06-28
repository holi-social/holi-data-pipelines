import dataclasses
import pprint
import psycopg
import math

from psycopg import Connection


from typing import cast, Optional
from urllib.parse import quote

from prefect import task, flow
from prefect.states import Completed, Failed, State

from src.common.monitoring.flow import (
    on_cancellation_hooks,
    on_crashed_hooks,
    on_failure_hooks,
    on_completion_hooks,
)
from src.common.variables import secret_value, variable_value_as
from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.typesense_client import TypesenseClient, TypesenseClientConfig
from src.common.variables import variable_value
from src.pipelines.bigquery import (
    bigquery_okuna_dataset,
    tasks_with_classifications,
    volunteering_voltastics_classified_enriched_partitioned,
    volunteering_aktion_mensch_classified_enriched_partitioned,
)

logger = PipelineLogger(__name__)

PG_BQ_DEVIATION_TOLERANCE_FACTOR = 0.05

DEVIATION_TOLERANCE_FACTOR = 0
DEVIATION_TOLERANCE_IN_PERCENT = 0


def get_pg_connection() -> Connection:
    host = variable_value_as(str, "okuna_db_hostname")
    port = int(variable_value_as(str, "okuna_db_port"))
    database = variable_value_as(str, "okuna_db_name")
    username = secret_value("okuna-db-username")
    password = secret_value("okuna-db-password")
    logger.debug(f"Trying to connect to postgres://{host}:{port}/{database}")
    return psycopg.connect(f"host={host} port={port} dbname={database} user={username} password={password}")


@task
def get_profile_ids_pg() -> list[str]:
    with get_pg_connection() as conn:
        with conn.cursor() as cursor:
            query = """
                SELECT au.username
                FROM openbook_auth_user au
                LEFT OUTER JOIN openbook_auth_userprofile p
                ON au.id = p.user_id
                WHERE
                    au.is_deleted = false
                    AND au.identity IS NOT NULL
                    AND (p.name IS NOT NULL OR p.last_name IS NOT NULL)
            """
            cursor.execute(query)
            results = cursor.fetchall()
            usernames = [row[0] for row in results]
            return usernames


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
def get_space_ids_pg() -> list[str]:
    with get_pg_connection() as conn:
        with conn.cursor() as cursor:
            query = """
                SELECT id
                FROM openbook_communities_community
                WHERE is_deleted = false
            """
            cursor.execute(query)
            results = cursor.fetchall()
            space_ids = [str(row[0]) for row in results]
            return space_ids


@task
def get_space_ids_bq() -> list[str]:
    result = extract_data_from_bigquery(f"""
        SELECT id
        FROM `{bigquery_okuna_dataset()}.openbook_communities_community`
        WHERE is_deleted=false
    """)  # noqa: S608
    return cast(list[str], [row["id"] for row in result])


@task
def get_task_ids_pg() -> list[str]:
    with get_pg_connection() as conn:
        with conn.cursor() as cursor:
            query = """
                SELECT task.id
                FROM openbook_communities_task AS task
                JOIN openbook_communities_community AS community ON task.community_id = community.id
                WHERE
                    community.is_deleted IS FALSE
                    AND task.visibility = 'PUBLIC'
                    AND task.deleted_at IS NULL
            """
            cursor.execute(query)
            results = cursor.fetchall()
            task_ids = [str(row[0]) for row in results]
            return task_ids


@task
def get_task_ids_bq() -> list[str]:
    result = extract_data_from_bigquery(f"""
        SELECT id
        FROM `{tasks_with_classifications()}`
    """)  # noqa: S608
    return cast(list[str], [row["id"] for row in result])


@task
def get_volunteering_ids_bq() -> list[str]:
    result_voltastics = extract_data_from_bigquery(f"""
        SELECT id
        FROM `{volunteering_voltastics_classified_enriched_partitioned()}`
        WHERE TIMESTAMP_TRUNC(partitiontime, DAY) = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
    """)  # noqa: S608
    result_aktion_mensch = extract_data_from_bigquery(f"""
        SELECT id
        FROM `{volunteering_aktion_mensch_classified_enriched_partitioned()}`
        WHERE TIMESTAMP_TRUNC(partitiontime, DAY) = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
    """)  # noqa: S608
    return cast(
        list[str], [str(row["id"]) for row in result_voltastics] + [str(row["id"]) for row in result_aktion_mensch]
    )


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


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def typesense_qc_flow() -> State:
    ts_client = TypesenseClient(TypesenseClientConfig.from_env())
    ts_collection = cast(str, variable_value("typesense_collection"))

    profile_ids_pg = get_profile_ids_pg()
    profile_ids_bq = get_profile_ids_bq()
    profile_ids_ts = get_ids_ts(ts_client, ts_collection, "profile")

    space_ids_pg = get_space_ids_pg()
    space_ids_bq = get_space_ids_bq()
    space_ids_ts = get_ids_ts(ts_client, ts_collection, "space")

    task_ids_pg = get_task_ids_pg()
    task_ids_bq = get_task_ids_bq()
    task_ids_ts = get_ids_ts(ts_client, ts_collection, "task")

    volunteering_ids_bq = get_volunteering_ids_bq()
    volunteering_ids_ts = get_ids_ts(ts_client, ts_collection, "volunteering")

    @dataclasses.dataclass
    class ConsistencyCheckResult:
        pg_count: Optional[int]
        bq_count: int
        ts_count: int
        pg_ids_not_in_bq: Optional[set[str]]
        bq_ids_not_in_pg: Optional[set[str]]
        bq_ids_not_in_ts: set[str]
        ts_ids_not_in_bq: set[str]

        def passes_consistency_check(self) -> bool:
            all_bq_docs_in_ts = self.bq_ids_not_in_ts == set()
            not_too_many_docs_in_ts = self.ts_ids_not_in_bq == set()
            processed_through_events_not_yet_in_bq = self.ts_ids_not_in_bq == self.pg_ids_not_in_bq
            pg_bq_lags_too_much = (
                ConsistencyCheckResult.fails_pg_bq_tolerance(
                    count=self.pg_count,
                    count_failed=len(self.pg_ids_not_in_bq or set()),
                )
                if self.pg_count
                else False
            )
            bq_consistent_with_pg = self.bq_ids_not_in_pg is None or self.bq_ids_not_in_pg == set()

            return (
                all_bq_docs_in_ts
                and (not_too_many_docs_in_ts or processed_through_events_not_yet_in_bq)
                and not pg_bq_lags_too_much
                and bq_consistent_with_pg
            )

        @staticmethod
        def fails_pg_bq_tolerance(count: int, count_failed: int) -> bool:
            return not math.isclose(count, count - count_failed, rel_tol=PG_BQ_DEVIATION_TOLERANCE_FACTOR)

    results = {
        "profile": ConsistencyCheckResult(
            pg_count=len(profile_ids_pg),
            bq_count=len(profile_ids_bq),
            ts_count=len(profile_ids_ts),
            pg_ids_not_in_bq=set(profile_ids_pg) - set(profile_ids_bq),
            bq_ids_not_in_pg=set(profile_ids_bq) - set(profile_ids_pg),
            bq_ids_not_in_ts=set(profile_ids_bq) - set(profile_ids_ts),
            ts_ids_not_in_bq=set(profile_ids_ts) - set(profile_ids_bq),
        ),
        "space": ConsistencyCheckResult(
            pg_count=len(space_ids_pg),
            bq_count=len(space_ids_bq),
            ts_count=len(space_ids_ts),
            pg_ids_not_in_bq=set(space_ids_pg) - set(space_ids_bq),
            bq_ids_not_in_pg=set(space_ids_bq) - set(space_ids_pg),
            bq_ids_not_in_ts=set(space_ids_bq) - set(space_ids_ts),
            ts_ids_not_in_bq=set(space_ids_ts) - set(space_ids_bq),
        ),
        "task": ConsistencyCheckResult(
            pg_count=len(task_ids_pg),
            bq_count=len(task_ids_bq),
            ts_count=len(task_ids_ts),
            pg_ids_not_in_bq=set(task_ids_pg) - set(task_ids_bq),
            bq_ids_not_in_pg=set(task_ids_bq) - set(task_ids_pg),
            bq_ids_not_in_ts=set(task_ids_bq) - set(task_ids_ts),
            ts_ids_not_in_bq=set(task_ids_ts) - set(task_ids_bq),
        ),
        "volunteering": ConsistencyCheckResult(
            pg_count=None,
            bq_count=len(volunteering_ids_bq),
            ts_count=len(volunteering_ids_ts),
            pg_ids_not_in_bq=None,
            bq_ids_not_in_pg=None,
            bq_ids_not_in_ts=set(volunteering_ids_bq) - set(volunteering_ids_ts),
            ts_ids_not_in_bq=set(volunteering_ids_ts) - set(volunteering_ids_bq),
        ),
    }

    all_types = ["profile", "space", "task", "volunteering"]
    errors = []

    for typ in all_types:
        if not results[typ].passes_consistency_check():
            msg = f"BigQuery and Typesense are inconsistent for document type {typ}%: {pprint.pformat(results[typ])}"
            logger.error(msg)
            errors.append(msg)

    if errors:
        return Failed(message="\n".join(errors))

    logger.info(f"Counts are within tolerance range of {DEVIATION_TOLERANCE_IN_PERCENT}% deviation: {results}")

    return Completed()


if __name__ == "__main__":
    typesense_qc_flow()
