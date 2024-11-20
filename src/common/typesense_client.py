import json
import typesense
from typing import Dict
from src.common.variables import variable_value
import prefect

# Initialize logging
logger = prefect.logging.get_logger()


class TypesenseClient:
    def __init__(self, config: Dict[str, str]) -> None:
        """
        Initialize TypesenseClient with a given configuration.

        Args:
            config (dict): Configuration dictionary to initialize the Typesense client.
        """
        self.client = self._initialize_typesense_client(config)

    def _initialize_typesense_client(self, config: Dict[str, str]) -> typesense.Client:
        """
        Initialize and return a Typesense client to connect to the Typesense server.

        Args:
            config (dict): Configuration dictionary containing Typesense connection details.

        Raises:
            ValueError: If required configuration values are missing or invalid.
            Exception: If any other issue arises while creating the client.

        Returns:
            typesense.Client: Configured Typesense client.
        """
        try:
            # Fetch necessary variables
            typesense_api_key = variable_value("typesense_api_key")
            typesense_host = variable_value("typesense_host", "localhost")
            typesense_port = variable_value("typesense_port", 8108)
            typesense_protocol = variable_value("typesense_protocol", "http")

            # Validate the API key
            if not typesense_api_key:
                raise ValueError("TYPESENSE_API_KEY is required in the configuration.")

            # Create the client
            client = typesense.Client(
                {
                    "api_key": typesense_api_key,
                    "nodes": [
                        {
                            "host": typesense_host,
                            "port": typesense_port,
                            "protocol": typesense_protocol,
                        }
                    ],
                    "connection_timeout_seconds": 60 * 60,
                }
            )

            logger.info("Typesense client successfully created.")
            return client

        except ValueError as ve:
            logger.error(f"Configuration error in Typesense client setup: {ve}")
            raise
        except Exception as e:
            logger.exception("Unexpected error creating Typesense client.")
            raise

    def get_client(self) -> typesense.Client:
        """
        Return the Typesense client for direct use.

        Returns:
            typesense.Client: The Typesense client instance.
        """
        return self.client

    def create_typesense_schema(self, schema_file_path: str = "all.json") -> None:
        """
        Create or update the Typesense schema from an external JSON file.

        Args:
            schema_file_path (str): Path to the schema JSON file.
        """
        try:
            # Load the schema from the JSON file
            with open(schema_file_path, "r") as file:
                schema = json.load(file)

            collection_name = schema.get("name")
            if not collection_name:
                raise ValueError("Schema is missing a 'name' field.")

            # Try deleting the existing collection if it exists
            try:
                self.client.collections[collection_name].delete()
            except typesense.exceptions.ObjectNotFound:
                pass  # Collection does not exist, ignore this error

            # Create the collection with the new schema
            self.client.collections.create(schema)
            logger.info(f"Typesense schema for '{collection_name}' created/updated successfully.")

            return collection_name

        except Exception as e:
            logger.error(f"Error creating/updating Typesense schema: {e}", exc_info=True)
            raise
