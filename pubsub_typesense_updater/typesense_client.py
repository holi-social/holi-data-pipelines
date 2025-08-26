from logging import getLogger
from typing import Any

from typesense.client import Client

logger = getLogger(__name__)


class TypesenseClient(object):
    """
    Thin wrapper around the Typesense client to simplify call site and testing/mocking code.
    """

    client: Client
    collection_name: str

    def __init__(self, host: str, port: str, protocol: str, api_key: str, collection_name: str) -> None:
        self.client = Client(
            {
                "api_key": api_key,
                "nodes": [
                    {
                        "host": host,
                        "port": port,
                        "protocol": protocol,
                    }
                ],
                "connection_timeout_seconds": 60 * 60,
            }
        )
        self.collection_name = collection_name

    def upsert(self, document: Any, params: Any | None = None) -> object:
        """
        Upserts the document in Typesense

        :param document: the document to upsert
        :param params: params dict to pass to actual Typesense client (e.g. filter_by)
        :return: deserialized JSON document
        """
        try:
            return self.client.collections[self.collection_name].documents.upsert(document, params)
        except Exception as e:
            # typesense at this point should only contain public data so it is fine to log documents (no PII)
            # if PII should be added later, this must be handled in the documents __str__ function
            logger.error(f"Error upserting document: {document}")
            raise e

    def delete(self, id: str) -> object:
        """
        Deletes the document from Typesense

        :param id: the id of the document to delete
        :return: deserialized JSON document
        """
        return self.client.collections[self.collection_name].documents[id].delete()
