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
        """
        Deletes a Typesense collection if it exists.

        This method attempts to delete a specified collection from the Typesense client.
        If the collection does not exist, it logs a debug message and continues.

        Args:
            collection_name (str): The name of the Typesense collection to delete.
        """
        try:
            self.client.collections[collection_name].delete()
            logger.info(f"Typesense collection '{collection_name}' successfully deleted.")
        except ObjectNotFound:
            logger.debug(f"Collection '{collection_name}' not found. Skipping deletion.")

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

    def get_active_collection(self, alias_name: str) -> str:
        """Retrieve the current active collection for the given alias name.

        Args:
            alias_name (str): The name of the alias to look up.

        Returns:
            str: The name of the collection associated with the alias, or an empty string if the alias does not exist.
        """
        try:
            alias_info = self.client.aliases[alias_name].retrieve()
            return alias_info.get("collection_name", "")
        except Exception as e:
            return ""
