from datetime import datetime

from prefect import flow, task
from prefect.logging import get_run_logger, get_logger
from prefect.artifacts import create_markdown_artifact
from prefect_gcp.bigquery import BigQueryWarehouse

from variables import variable_value


@task
def get_number_of_volunteering_opportunities(day_as_string: str) -> int:
    with BigQueryWarehouse.load("bigquery-warehouse") as warehouse:
        table_name = variable_value("volunteering_voltastics_raw_table")
        result = warehouse.fetch_one(
            f"""
        SELECT
          count(*) as amount FROM `{table_name}` 
        WHERE
          TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(%(day)s)
        """,
            parameters={"day": day_as_string, "table_name": table_name},
        )
        return result.get("amount")


@task
def mark_it_down(day_as_string: str, count: int) -> None:
    markdown_report = f"""# Number of volunteering opportunities

| Time      | # of opportunities |
| :-------- | ----------: |
| {day_as_string}|      {count} |
"""
    create_markdown_artifact(
        key="volunteering-report",
        markdown=markdown_report,
        description="the number of volunteering opportunities",
    )


# TODO task caching: https://docs.prefect.io/3.0/develop/task-caching

@flow(timeout_seconds=300)
def bigquery_report() -> None:
    """
    A flow for generating a report of how many current volunteering opportunities are available in BigQuery.

    :return:
    """
    run_logger = get_run_logger()
    logger = get_logger()
    run_logger.info("I am logging to prefect!")
    logger.info("I am logging to GCP!")
    day_as_string = datetime.today().strftime("%Y-%m-%d")
    no_of_opportunities = get_number_of_volunteering_opportunities(day_as_string)
    mark_it_down(day_as_string, no_of_opportunities)


if __name__ == "__main__":
    bigquery_report()
