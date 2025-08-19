from prefect import flow
from prefect.states import State, Failed, Completed
from src.pipelines.crawlers.betterplace.pipeline import crawl_betterplace
from src.common.constants import VIEW_PARTITION_TIME_COLUMN_NAME
from src.common.extractors.bq_extractor import extract_data_from_bigquery
from src.pipelines.bigquery import donations_betterplace_raw
from src.pipelines.crawlers.crawler_common_code import get_last_successful_day
from src.common.monitoring.flow import (
    on_cancellation_hooks,
    on_crashed_hooks,
    on_failure_hooks,
    on_completion_hooks,
)


@flow(
    on_cancellation=on_cancellation_hooks,
    on_crashed=on_crashed_hooks,
    on_failure=on_failure_hooks,
    on_completion=on_completion_hooks,
)
def donation_updates_flow() -> State:
    """
    Automation graph to emit donation updated, created and deleted events to pubsub.
    """
    old_partition_date = get_last_successful_day()

    betterplace_crawl_result = crawl_betterplace()

    new_partition_date = get_last_successful_day()

    if new_partition_date is None or old_partition_date == new_partition_date:
        return Failed(msg="Crawler run was not successful")

    donations_table_name = donations_betterplace_raw()

    changes = extract_data_from_bigquery(f"""
      WITH old_partition AS (
        SELECT
          r.*,
          FARM_FINGERPRINT(TO_JSON_STRING(r)) AS row_hash
        FROM `{donations_table_name}` AS r
        WHERE TIMESTAMP_TRUNC({VIEW_PARTITION_TIME_COLUMN_NAME}, DAY) = TIMESTAMP('{old_partition_date or "1970-01-01"}')
        AND completed_at IS NULL
        AND closed_at IS NULL
        AND donations_prohibited IS false
      ),
      new_partition AS (
        SELECT
          r.*,
          FARM_FINGERPRINT(TO_JSON_STRING(r)) AS row_hash
        FROM `{donations_table_name}` AS r
        WHERE TIMESTAMP_TRUNC({VIEW_PARTITION_TIME_COLUMN_NAME}, DAY) = TIMESTAMP('{new_partition_date}')
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

    # alter crawler code: https://gitlab.holi.team/attic/holi-data-crawler/-/blob/main/app/betterplace_crawler.ts?ref_type=heads

    # old filter query: https://gitlab.holi.team/app/holi-data-classification/-/blob/main/terraform/environments/bigquery.tf?ref_type=heads
    # Filter donation projects that can no longer be donated for
    # AND raw.completed_at IS NULL
    # AND raw.closed_at IS NULL
    # AND raw.donations_prohibited IS false


if __name__ == "__main__":
    donation_updates_flow()
