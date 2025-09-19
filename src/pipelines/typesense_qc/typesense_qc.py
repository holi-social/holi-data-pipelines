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
    donations_betterplace_raw,
)

logger = PipelineLogger(__name__)

PG_BQ_DEVIATION_TOLERANCE_FACTOR = 0.05

BQ_TS_DEVIATION_TOLERANCE_FACTOR = 0.01
BQ_TS_DEVIATION_TOLERANCE_IN_PERCENT = BQ_TS_DEVIATION_TOLERANCE_FACTOR * 100


@dataclasses.dataclass
class ConsistencyCheck:
    pg_ids: Optional[list[str]]
    bq_ids: list[str]
    ts_ids: list[str]

    def pg_count(self) -> int:
        return len(self.pg_ids) if self.pg_ids else 0

    def bq_count(self) -> int:
        return len(self.bq_ids)

    def ts_count(self) -> int:
        return len(self.ts_ids)

    def pg_ids_not_in_bq(self) -> set[str]:
        return set(self.pg_ids) - set(self.bq_ids) if self.pg_ids else set()

    def bq_ids_not_in_pg(self) -> set[str]:
        return set(self.bq_ids) - set(self.pg_ids) if self.pg_ids else set()

    def bq_ids_not_in_ts(self) -> set[str]:
        return set(self.bq_ids) - set(self.ts_ids)

    def ts_ids_not_in_bq(self) -> set[str]:
        return set(self.ts_ids) - set(self.bq_ids)

    def __repr__(self) -> str:
        return str(
            {
                "pg_ids_not_in_bq": sorted(self.pg_ids_not_in_bq()),
                "bq_ids_not_in_pg": sorted(self.bq_ids_not_in_pg()),
                "bq_ids_not_in_ts": sorted(self.bq_ids_not_in_ts()),
                "ts_ids_not_in_bq": sorted(self.ts_ids_not_in_bq()),
            }
        )

    def passes_consistency_check(self) -> bool:
        bq_ts_diff_too_large = ConsistencyCheck.fails_bq_ts_tolerance(
            count=self.bq_count(), count_failed=(len(self.bq_ids_not_in_ts()) + len(self.ts_ids_not_in_bq()))
        )

        diff_pg_bq = self.pg_ids_not_in_bq().union(self.bq_ids_not_in_pg())
        diff_bq_ts = self.bq_ids_not_in_ts().union(self.ts_ids_not_in_bq())
        processed_through_events_not_yet_in_bq = diff_pg_bq == diff_bq_ts

        pg_bq_lags_too_much = (
            ConsistencyCheck.fails_pg_bq_tolerance(
                count=self.pg_count(),
                count_failed=len(diff_pg_bq),
            )
            if self.pg_count()
            else False
        )

        return (not bq_ts_diff_too_large or processed_through_events_not_yet_in_bq) and not pg_bq_lags_too_much

    @staticmethod
    def fails_pg_bq_tolerance(count: int, count_failed: int) -> bool:
        return not math.isclose(count, count - count_failed, rel_tol=PG_BQ_DEVIATION_TOLERANCE_FACTOR)

    @staticmethod
    def fails_bq_ts_tolerance(count: int, count_failed: int) -> bool:
        return not math.isclose(count, count - count_failed, rel_tol=BQ_TS_DEVIATION_TOLERANCE_FACTOR)


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
def get_donation_ids_bq() -> list[str]:
    result = extract_data_from_bigquery(f"""
        SELECT id
        FROM `{donations_betterplace_raw()}`
        WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
        AND completed_at IS NULL
        AND closed_at IS NULL
        AND donations_prohibited IS false
    """)  # noqa: S608
    return cast(list[str], [str(row["id"]) for row in result])


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

    checks = {
        "profile": ConsistencyCheck(
            pg_ids=get_profile_ids_pg(),
            bq_ids=get_profile_ids_bq(),
            ts_ids=get_ids_ts(ts_client, ts_collection, "profile"),
        ),
        "space": ConsistencyCheck(
            pg_ids=get_space_ids_pg(),
            bq_ids=get_space_ids_bq(),
            ts_ids=get_ids_ts(ts_client, ts_collection, "space"),
        ),
        "task": ConsistencyCheck(
            pg_ids=get_task_ids_pg(),
            bq_ids=get_task_ids_bq(),
            ts_ids=get_ids_ts(ts_client, ts_collection, "task"),
        ),
        "volunteering": ConsistencyCheck(
            pg_ids=None,
            bq_ids=get_volunteering_ids_bq(),
            ts_ids=get_ids_ts(ts_client, ts_collection, "volunteering"),
        ),
        "donation": ConsistencyCheck(
            pg_ids=None,
            bq_ids=get_donation_ids_bq(),
            ts_ids=get_ids_ts(ts_client, ts_collection, "donation"),
        ),
    }

    errorMessages = []

    for checkType, check in checks.items():
        if check.passes_consistency_check():
            logger.info(f"Consistency checks succeeded for type {checkType}")
        else:
            msg = f"BigQuery and Typesense are inconsistent for document type {checkType}: {pprint.pformat(check)}"
            logger.error(msg)
            errorMessages.append(msg)

    if errorMessages:
        return Failed(message="\n".join(errorMessages))

    logger.info(f"Counts are within tolerance range of {BQ_TS_DEVIATION_TOLERANCE_IN_PERCENT}% deviation: {checks}")

    return Completed()


if __name__ == "__main__":
    typesense_qc_flow()
