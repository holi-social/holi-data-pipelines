import argparse
from datetime import date
from prefect import flow
from prefect.states import State, Completed
from typing import Literal, Optional
from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.pipelines.bigquery import donations_betterplace_raw
from src.common.monitoring.flow import (
    on_cancellation_hooks,
    on_crashed_hooks,
    on_failure_hooks,
    on_completion_hooks,
)
from shared.events.event import Donation
from src.common.logging.PipelineLogger import PipelineLogger

ChangeType = Literal["created", "updated", "deleted"]

logger = PipelineLogger(__name__)


def extract_changes_from_bigquery(
    old_partition_date: Optional[date], new_partition_date: date
) -> list[tuple[ChangeType, Donation]]:
    donations_table_name = donations_betterplace_raw()

    changes = extract_data_from_bigquery(f"""
      WITH old_partition AS (
        SELECT
          r.*,
          FARM_FINGERPRINT(TO_JSON_STRING(r)) AS row_hash
        FROM `{donations_table_name}` AS r
        WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP('{str(old_partition_date or "1970-01-01")}')
        AND completed_at IS NULL
        AND closed_at IS NULL
        AND donations_prohibited IS false
      ),
      new_partition AS (
        SELECT
          r.*,
          FARM_FINGERPRINT(TO_JSON_STRING(r)) AS row_hash
        FROM `{donations_table_name}` AS r
        WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP('{str(new_partition_date)}')
        AND completed_at IS NULL
        AND closed_at IS NULL
        AND donations_prohibited IS false
      ),
      new_records AS (
        SELECT * EXCEPT (row_hash)
        FROM new_partition
        WHERE id NOT IN (
          SELECT id
          FROM old_partition
        )
      ),
      updated_records AS (
        SELECT n.* EXCEPT (row_hash)
        FROM new_partition AS n
        INNER JOIN old_partition AS o
        ON n.id = o.id
        WHERE n.row_hash != o.row_hash
      ),
      deleted_records AS (
        SELECT * EXCEPT (row_hash)
        FROM old_partition
        WHERE id NOT IN (
          SELECT id
          FROM new_partition
        )
      )
    SELECT
      'created' AS change_type,
      *
    FROM new_records
    UNION ALL
    SELECT
      'updated' AS change_type,
      *
    FROM updated_records
    UNION ALL
    SELECT
        'deleted' AS change_type,
        *
    FROM deleted_records
    """)  # noqa: S608

    return [(record["change_type"], Donation(**record)) for record in changes]


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def donation_events_flow(old_partition_date: Optional[date], new_partition_date: date) -> State:
    """
    Automation graph to emit donation updated, created and deleted events to pubsub.
    """
    changes = extract_changes_from_bigquery(old_partition_date, new_partition_date)
    counts = {"created": 0, "updated": 0, "deleted": 0}
    for change, _ in changes:
        counts[change] = counts[change] + 1
    logger.info(
        f"{len(changes)} total changes, {counts['created']} creations, {counts['updated']} updates, {counts['deleted']} deletions"
    )
    return Completed()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--old_partition_date",
        "--old",
        "-o",
        type=date.fromisoformat,
        required=True,
        help="Old partition date in ISO format (e.g., 2024-01-15)",
    )

    parser.add_argument(
        "--new_partition_date",
        "--new",
        "-n",
        type=date.fromisoformat,
        required=True,
        help="New partition date in ISO format (e.g., 2024-01-16)",
    )

    args = parser.parse_args()

    logger.info(f"Old partition date: {args.old_partition_date}, new partition date: {args.new_partition_date}")

    donation_events_flow(args.old_partition_date, args.new_partition_date)
