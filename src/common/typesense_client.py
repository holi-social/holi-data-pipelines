import json
import logging
import typesense
from typing import Dict, Optional
from common.logging_setup import initialize_logging

# Initialize logging
initialize_logging()
logger = logging.getLogger(__name__)


class TypesenseClient:
    def __init__(self, config: Dict[str, str]) -> None:
        """
        Initialize TypesenseManager with a given configuration.

        Args:
            config (dict): Configuration dictionary to initialize the Typesense client.
        """
        self.client = self.get_typesense_client(config)

    def get_typesense_client(self, config: Dict[str, str]) -> typesense.Client:
        """
        Initialize and return a Typesense client to connect to the Typesense server.

        The client configuration is passed via the `config` dictionary.

        Args:
            config (dict): The configuration dictionary containing Typesense connection details.

        Raises:
            ValueError: If required Typesense configuration values are missing or invalid.
            Exception: If any other issue arises while creating the client.

        Returns:
            typesense.Client: Configured Typesense client.
        """
        try:
            typesense_api_key = config.get("TYPESENSE_API_KEY")
            typesense_host = config.get("TYPESENSE_HOST", "localhost")
            typesense_port = int(config.get("TYPESENSE_PORT", 8108))
            typesense_protocol = config.get("TYPESENSE_PROTOCOL", "http")

            if not typesense_api_key:
                raise ValueError("TYPESENSE_API_KEY is required in the configuration.")

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
                    "connection_timeout_seconds": 10,
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
            logger.info(
                f"Typesense schema for '{collection_name}' created/updated successfully."
            )

        except Exception as e:
            logger.error(
                f"Error creating/updating Typesense schema: {e}", exc_info=True
            )
            raise
