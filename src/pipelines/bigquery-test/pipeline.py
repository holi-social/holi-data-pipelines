import datetime
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from variables import resolve
from prefect_gcp.bigquery import BigQueryWarehouse


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
        print(f"table_name: {table_name}\nday_as_string: {day_as_string}")
        result = warehouse.fetch_one(
            """
        SELECT
          count(*) FROM `%(table_name)s` 
        WHERE
          TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(%(day)s)
        """,
            parameters={"day": day_as_string, "table_name": table_name},
        )
        print(f"result: {result}")
        mark_it_down(day_as_string, result)


if __name__ == "__main__":
    bigquery_report()
