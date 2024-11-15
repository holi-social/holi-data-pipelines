import logging
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.auth.exceptions import DefaultCredentialsError
import pandas as pd
from typing import Dict, Union, Optional
from common.logging_setup import initialize_logging

# Initialize logging from logging_setup
initialize_logging()
logger = logging.getLogger(__name__)

class BigQueryExtractor:
    def __init__(self, project_id: str):
        """
        Initializes BigQuery client using the project ID from configuration.
        
        Raises:
            ValueError: If PROJECT_ID is not set in the configuration.
            DefaultCredentialsError: If Google Cloud credentials are not set.
        """
        self.project_id = project_id
        if not self.project_id:
            raise ValueError("PROJECT_ID is required.")
        
        try:
            # Initialize BigQuery client
            self.client = bigquery.Client(project=self.project_id)
            logger.info("BigQuery client initialized successfully.", extra={"project_id": self.project_id})
        except DefaultCredentialsError as e:
            logger.error("Google Cloud credentials not found.", exc_info=True)
            raise e
    
    def extract_data_from_bigquery(self, query: Optional[str] = None, table_name: Optional[str] = None, return_format: str = "dict_list") -> Union[pd.DataFrame, list[Dict]]:
        """
        Extracts data from BigQuery based on a SQL query or table name using the initialized BigQuery client.

        Args:
            query (Optional[str]): Custom SQL query to execute. Overrides `table_name` if provided.
            table_name (Optional[str]): Fully-qualified table name in `project.dataset.table` format.
            return_format (str): The format for returning results, either 'dataframe' or 'dict_list'.

        Returns:
            Union[pd.DataFrame, list[Dict]]: Query results in the specified format.

        Raises:
            ValueError: If neither `query` nor `table_name` is provided, or if `return_format` is invalid.
            GoogleAPIError: If the BigQuery query fails.
        """
        # Determine the query to run
        if query:
            sql_query = query
        elif table_name:
            sql_query = f"SELECT * FROM `{table_name}`"
        else:
            raise ValueError("Either 'query' or 'table_name' must be provided.")

        logger.info("Executing BigQuery query.", extra={"query": sql_query})
        
        try:
            query_job = self.client.query(sql_query)
            result = query_job.result()  # Waits for the query to complete

            if return_format == "dataframe":
                dataframe = result.to_dataframe()
                logger.info("Query executed successfully, returning DataFrame.", extra={"num_rows": len(dataframe)})
                return dataframe
            elif return_format == "dict_list":
                dict_list = [dict(row) for row in result]
                logger.info("Query executed successfully, returning list of dictionaries.", extra={"num_rows": len(dict_list)})
                return dict_list
            else:
                raise ValueError("Invalid return_format. Choose 'dataframe' or 'dict_list'.")

        except (GoogleAPIError, NotFound) as e:
            logger.error("Query execution failed.", extra={"error": str(e), "query": sql_query}, exc_info=True)
            raise e