from typing import Dict, Any, Optional

import prefect.logging
from prefect_gcp import BigQueryWarehouse
from datetime import datetime, date
from src.common.constants import VIEW_PARTITION_TIME_COLUMN_NAME

logger = prefect.logging.get_logger()


def extract_data_from_bigquery(
    query: str,
) -> list[Dict[str, Any]]:
    """
    Extracts data from BigQuery based on a SQL query using the initialized BigQuery client.

    Args:
        query (str): Custom SQL query to execute.

    Returns:
        list[Dict]: Query results.

    Raises:
        GoogleAPIError: If the BigQuery query fails.
    """
    # Determine the query to run
    logger.info("Executing BigQuery query.", extra={"query": query})

    with BigQueryWarehouse.load("bigquery-warehouse") as warehouse:
        result = warehouse.fetch_all(query)

        dict_list = [dict(row) for row in result]
        logger.info(
            "Query executed successfully, returning list of dictionaries.",
            extra={"num_rows": len(dict_list)},
        )
        return dict_list

def extract_latest_partition_timestamp_from_bigquery(
    table_name: str,
) -> datetime:
    """
    Extracts the latest partition timestamp from a BigQuery table.

    Args:
        table_name (str): The name of the BigQuery table.

    Returns:
        list[Dict]: A list containing a single dictionary with the latest partition timestamp.
    """
    query = (
        f"SELECT MAX(`{VIEW_PARTITION_TIME_COLUMN_NAME}`) AS `{VIEW_PARTITION_TIME_COLUMN_NAME}` "  # noqa: S608
        f"FROM `{table_name}`"
    )
    result = extract_data_from_bigquery(query)
    if not result:
        raise ValueError("No data returned from the query.")

    latest_partition_timestamp = result[0].get(VIEW_PARTITION_TIME_COLUMN_NAME)

    if latest_partition_timestamp is None:
        raise ValueError(f"Partition column `{VIEW_PARTITION_TIME_COLUMN_NAME}` returned no value.")

    return latest_partition_timestamp

def extract_changes_in_partitions(
        table_name: str,
        new_partition_date: date,
        old_partition_date: Optional[date] = None
) -> list[Dict[str, Any]]:
    """
    Extracts (new, updated, or deleted records) in a BigQuery table by comparing
    the old partition with the new partition timestamp. It requires the table to have a hash
    column which is used for the comparison of rows.

    Args:
        table_name (str): The BigQuery view or table name to query.
        new_partition_date (date): The date of the newer partition
        old_partition_date (date): The date of the older partition

    Returns:
        list[Dict]: A list of dictionaries, where each dictionary represents a record
                    with an additional `change_type` field indicating whether it is
                    "new_or_updated" or "deleted".
    """

    query = f"""
    WITH old_partition AS (
        SELECT *
        FROM `{table_name}`
        WHERE TIMESTAMP_TRUNC({VIEW_PARTITION_TIME_COLUMN_NAME}, DAY) = TIMESTAMP('{old_partition_date.strftime("%Y-%m-%d") if old_partition_date else "1970-01-01"}')
    ),
    new_partition AS (
        SELECT *
        FROM `{table_name}`
        WHERE TIMESTAMP_TRUNC({VIEW_PARTITION_TIME_COLUMN_NAME}, DAY) = TIMESTAMP('{new_partition_date.strftime("%Y-%m-%d")}')
    ),
    new_or_updated_records AS (
        SELECT *
        FROM new_partition
        WHERE `hash` NOT IN (
            SELECT `hash`
            FROM old_partition
        )
    ),
    deleted_records AS (
        SELECT *
        FROM old_partition
        WHERE `hash` NOT IN (
            SELECT `hash`
            FROM new_partition
        )
    )
    SELECT
        'new_or_updated' AS change_type
        ,*
    FROM new_or_updated_records
    UNION ALL
    SELECT
        'deleted' AS change_type
        ,*
    FROM deleted_records;
    """  # noqa: S608
    return extract_data_from_bigquery(query)


def extract_data_from_bigquery_table(table_name: str) -> list[Dict[str, Any]]:
    """
    Extracts data from BigQuery based on a table name (select * from ...) using the initialized BigQuery client.

    Args:
        table_name (str): The table to query.

    Returns:
        list[Dict]: Query results.

    Raises:
        GoogleAPIError: If the BigQuery query fails.
    """
    return extract_data_from_bigquery(f"SELECT * FROM `{table_name}`")  # noqa: S608
