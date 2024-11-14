from datetime import datetime

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_gcp.bigquery import BigQueryWarehouse

from variables import resolve


@task
def mark_it_down(day_as_string, count):
    markdown_report = f"""# Number of volunteering opportunities

| Time      | # of opportunities |
| :-------- | ----------: |
| {day_as_string}|      {count} |
"""
    create_markdown_artifact(
        key="weather-report",
        markdown=markdown_report,
        description="Very scientific weather report",
    )


@flow
def bigquery_report():
    with BigQueryWarehouse.load("bigquery-warehouse") as warehouse:
        day_as_string = datetime.today().strftime("%Y-%m-%d")
        table_name = resolve("volunteering_voltastics_raw_table")
        result = warehouse.fetch_one(
            f"""
        SELECT
          count(*) as amount FROM `{table_name}` 
        WHERE
          TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(%(day)s)
        """,
            parameters={"day": day_as_string, "table_name": table_name},
        )
        mark_it_down(day_as_string, result.get("amount"))


if __name__ == "__main__":
    bigquery_report()
