import json
from dataclasses import dataclass
from typing import cast
from urllib.parse import quote

from typesense.client import Client
from typesense.exceptions import ObjectNotFound

from src.common.logging.PipelineLogger import PipelineLogger
from src.common.variables import secret_value, variable_value

logger = PipelineLogger(__name__)


@dataclass
class TypesenseClientConfig:
    api_key: str
    host: str
    port: str
    protocol: str

    @staticmethod
    def from_env() -> "TypesenseClientConfig":
        return TypesenseClientConfig(
            api_key=cast(str, secret_value("typesense-admin-api-key")),
            host=cast(str, variable_value("typesense_host")),
            port=cast(str, variable_value("typesense_port")),
            protocol=cast(str, variable_value("typesense_protocol")),
        )


class TypesenseClient:
    def __init__(self, config: TypesenseClientConfig) -> None:
        """
        Initialize TypesenseClient with a given configuration.

        Args:
            config (dict): Configuration dictionary to initialize the Typesense client.
        """
        self.client = Client(
            {
                "api_key": config.api_key,
                "nodes": [
                    {
                        "host": config.host,
                        "port": str(config.port),
                        "protocol": config.protocol,
                    }
                ],
                "connection_timeout_seconds": 60 * 60,
            }
        )

    def drop_collection(self, collection_name: str) -> None:
        """
        Deletes a Typesense collection if it exists.

        This method attempts to delete a specified collection from the Typesense client.
        If the collection does not exist, it logs a debug message and continues.

        Args:
            collection_name (str): The name of the Typesense collection to delete.
        """
        try:
            url_encoded_collection_name = quote(collection_name)
            self.client.collections[url_encoded_collection_name].delete()
            logger.info(f"Typesense collection '{collection_name}' successfully deleted.")
        except ObjectNotFound:
            logger.warning(f"Collection '{collection_name}' not found. Skipping deletion.")

    def create_collection(self, schema_file_path: str, collection_name: str) -> str:
        """
        Create or update the Typesense schema from an external JSON file.

        Returns: collection_name (str): name of the collection found in JSON file

        Args:
            schema_file_path (str): Path to the schema JSON file.
            collection_name (str): Name of the new collection (overwrites name in schema file).
        """
        # Load the schema from the JSON file
        with open(schema_file_path, "r") as file:
            schema = json.load(file)

        if not collection_name:
            raise ValueError("Missing 'collection_name' field.")

        schema["name"] = collection_name

        # Create the collection with the new schema
        self.client.collections.create(schema)
        logger.info(f"Typesense collection '{collection_name}' created/updated.")

        return collection_name

    def get_active_collection_name(self, alias_name: str) -> str:
        """Retrieve the current active collection for the given alias name.

        Args:
            alias_name (str): The name of the alias to look up.

        Returns:
            str: The name of the collection associated with the alias or an empty string.

        Raises:
            ValueError: If `alias_name` is None or empty.
            ObjectNotFound: If the alias does not exist.
        """
        if not alias_name:
            raise ValueError("alias_name must not be None or empty.")

        try:
            alias_info = self.client.aliases[alias_name].retrieve()
        except ObjectNotFound:
            logger.warning(f"Alias '{alias_name}' does not exist in Typesense.")
            raise
        return alias_info.get("collection_name", "")

    def set_active_collection(self, alias_name: str, collection_name: str) -> None:
        if not alias_name:
            raise ValueError("alias_name must not be None or empty.")
        if not collection_name:
            raise ValueError("collection_name must not be None or empty.")
        self.client.aliases.upsert(alias_name, {"collection_name": collection_name})
