import json
import typesense
from typing import Dict
import prefect
from typesense.client import Client  # type: ignore
from typesense.exceptions import ObjectNotFound  # type: ignore

# Initialize logging
logger = prefect.logging.get_logger()


class TypesenseClient:
    def __init__(self, config: Dict[str, str]) -> None:
        """
        Initialize TypesenseClient with a given configuration.

        Args:
            config (dict): Configuration dictionary to initialize the Typesense client.
        """

        # Fetch necessary variables
        typesense_api_key = config["api_key"]
        typesense_host = config["host"]
        typesense_port = config["port"]
        typesense_protocol = config["protocol"]
        # Create the client
        self.client = Client(
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

        logger.debug("Typesense client successfully initialized.")

    def drop_collection(self, collection_name: str):
        # Try deleting the existing collection if it exists
        try:
            self.client.collections[collection_name].delete()
            logger.info(f"Typesense collection '{collection_name}' deleted.")
        except ObjectNotFound:
            logger.debug("Couldn't find collection. Proceeding....")

    def create_collection(self, schema_file_path: str, collection_name: str):
        """
        Create or update the Typesense schema from an external JSON file.

        Returns: collection_name (str): name of the collection found in JSON file

        Args:
            schema_file_path (str): Path to the schema JSON file.
        """
        # Load the schema from the JSON file
        with open(schema_file_path, "r") as file:
            schema = json.load(file)

        if not collection_name:
            raise ValueError("Missing 'collection_name' field.")

        schema.set("name", collection_name)

        # Create the collection with the new schema
        self.client.collections.create(schema)
        logger.info(f"Typesense collection '{collection_name}' created/updated.")

        return collection_name
