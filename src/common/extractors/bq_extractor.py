from typing import Dict, Any

import prefect.logging
from prefect_gcp import BigQueryWarehouse

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
    return extract_data_from_bigquery(f"SELECT * FROM `{table_name}`")
