from typing import Dict

import prefect.logging
from prefect_gcp import BigQueryWarehouse

logger = prefect.logging.get_logger()


def extract_data_from_bigquery(
    query: str,
) -> list[Dict]:
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
    table_name: str, partitiontime_column_name: str
) -> list[Dict]:
    """
    Extracts the latest partition timestamp from a BigQuery table.

    Args:
        table_name (str): The name of the BigQuery table.
        partitiontime_column_name (str): The name of the partition time column in the table.

    Returns:
        list[Dict]: A list containing a single dictionary with the latest partition timestamp.
    """
    query = (
        f"SELECT MAX(`{partitiontime_column_name}`) AS `{partitiontime_column_name}` "
        f"FROM `{table_name}`"
    )
    return extract_data_from_bigquery(query)

def detect_changes_in_partitions(view_name: str, last_ingestion_timestamp: str) -> list[Dict]:
    """
    Detects changes (new, updated, or deleted records) in a BigQuery table by comparing
    the latest partition with the last successful ingestion timestamp.

    Args:
        view_name (str): The BigQuery view or table name to query.
        last_ingestion_timestamp (str): The timestamp of the last successful ingestion
                                         in the format "%Y-%m-%d".

    Returns:
        list[Dict]: A list of dictionaries, where each dictionary represents a record
                    with an additional `change_type` field indicating whether it is
                    "new_or_updated" or "deleted".
    """

    if not last_ingestion_timestamp:
        query = f"""
        WITH latest_partition AS (
        SELECT *
        FROM `{view_name}`
        WHERE
            partitiontime = (
                SELECT MAX(partitiontime)
                FROM `{view_name}`
                )
        )
        SELECT 'new_or_updated' AS change_type
            ,*
        FROM latest_partition;
        """
        return extract_data_from_bigquery(query)

    query = f"""
    WITH last_successful_partition AS (
        SELECT *
        FROM `{view_name}`
        WHERE TIMESTAMP_TRUNC(partitiontime, DAY) = TIMESTAMP('{last_ingestion_timestamp}')
    ),
    latest_partition AS (
        SELECT *
        FROM `{view_name}`
        WHERE
            partitiontime = (
                SELECT MAX(partitiontime)
                FROM `{view_name}`
                )
        ),
    new_or_updated_records AS (
        SELECT *
        FROM latest_partition
        WHERE `hash` NOT IN (
                SELECT `hash`
                FROM last_successful_partition
                )
        ),
    deleted_records AS (
        SELECT *
        FROM last_successful_partition
        WHERE `hash` NOT IN (
                SELECT `hash`
                FROM latest_partition
                )
        )
    SELECT 'new_or_updated' AS change_type
        ,*
    FROM new_or_updated_records
    UNION ALL
    SELECT 'deleted' AS change_type
        ,*
    FROM deleted_records;
    """
    return extract_data_from_bigquery(query)


def extract_data_from_bigquery_table(table_name: str) -> list[Dict]:
    """
    Extracts data from BigQuery based on a table name (select * from ...) using the initialized BigQuery client.

    Args:
        table_name (str): The table to query.

    Returns:
        list[Dict]: Query results.

    Raises:
        GoogleAPIError: If the BigQuery query fails.
    """
    return extract_data_from_bigquery(f"SELECT * FROM `{table_name}`")
