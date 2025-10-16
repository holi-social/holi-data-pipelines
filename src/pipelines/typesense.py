from dataclasses import dataclass
from functools import cache
from typing import cast
from urllib.parse import quote

from prefect import State, task
from prefect.states import Completed, Failed

from src.common.logging.PipelineLogger import PipelineLogger
from src.common.typesense_client import TypesenseClient
from src.common.variables import variable_value


# Schema path for typesense collections
@cache
def schema_path() -> str:
    return f"src/typesense/{cast(str, variable_value('prefect_environment'))}_all.json"


logger = PipelineLogger(__name__)


@dataclass
class TypesenseDocId:
    prefix: str
    id: str

    def __str__(self) -> str:
        return f"{self.prefix}_{self.id}"


@task
def delete_typesense_documents(
    typesense_client: TypesenseClient,
    typesense_collection_name: str,
    document_ids_to_delete: list[TypesenseDocId],
) -> bool:
    """Deletes documents with the given IDs from the Typesense collection.

    Args:
        typesense_client (TypesenseClient): Typesense client
        typesense_collection_name (str): name of the collection
        document_ids_to_delete (list(TypesenseDocId)): IDs of documents to delete

    Returns:
        bool: True if deletion was successful, False otherwise
    """
    filter_str = f"id: [{', '.join([str(doc_id) for doc_id in document_ids_to_delete])}]"

    try:
        url_encoded_collection_name = quote(typesense_collection_name)
        response = typesense_client.client.collections[url_encoded_collection_name].documents.delete(
            {"filter_by": filter_str}
        )

        num_deleted = response.get("num_deleted")
        deleted_all = num_deleted == len(document_ids_to_delete)

        if deleted_all:
            logger.info(f"Deleted {num_deleted} volunteering documents")
        else:
            missing = len(document_ids_to_delete) - num_deleted
            logger.error(
                f"Partial deletion: {num_deleted}/{len(document_ids_to_delete)} ({missing} missing). IDs: {document_ids_to_delete}"
            )
        return True
    except Exception as e:
        logger.error(
            f"Error deleting {len(document_ids_to_delete)} documents from Typesense: "
            + str(e)
            + "\nFailed document ids: "
            + ",".join([str(doc_id) for doc_id in document_ids_to_delete])
        )
        return False


@task
def validate_collection(typesense_client: TypesenseClient, collection_name: str) -> State:
    """Validate the new collection before switching the alias."""
    # test a sample search
    url_encoded_collection_name = quote(collection_name)
    sample_search = typesense_client.client.collections[url_encoded_collection_name].documents.search(
        {
            "q": "*",
            "query_by": "type",
            "page": 1,
            "per_page": 1,
        }
    )
    if not sample_search["hits"]:
        return Failed(message=f"No documents found for {collection_name}")

    return Completed()
