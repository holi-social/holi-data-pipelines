import json
import logging
from typing import List, Dict
from typesense.exceptions import TypesenseClientError
from common.logging_setup import initialize_logging

# Initialize logging from logging_setup
initialize_logging()
logger = logging.getLogger(__name__)


def index_documents_in_batches(
    client: object, docs: List[Dict[str, object]], batch_num: int, index_name: str
) -> List[Dict[str, object]]:
    """
    Index a batch of documents into Typesense using the upsert action.

    Args:
        client (object): The Typesense client instance used to interact with the Typesense API.
        docs (List[Dict[str, object]]): A list of dictionaries representing documents to be indexed.
                                        Each document is expected to have key-value pairs where keys are strings
                                        and values can be of any type (e.g., string, number, etc.).
        batch_num (int): The current batch number, used for logging and identifying the batch being processed.
        index_name (str): The name of the index to which the documents will be added or updated.

    Returns:
        List[Dict[str, object]]: A list of failed items (if any), where each item is a dictionary with information about the failure.

    Raises:
        TypesenseClientError: If there is an issue with the Typesense client or document import (e.g., network issues, invalid configuration).
        Exception: If any other unexpected error occurs during the batch processing.
    """
    try:
        # Import documents with upsert option
        response = client.collections[index_name].documents.import_(
            docs, {"action": "upsert", "dirty_values": "coerce_or_reject"}
        )

        # Parse and filter results
        results = [
            json.loads(line) if isinstance(line, str) else line for line in response
        ]
        failed_items = [item for item in results if not item.get("success", True)]
        success_count = len(results) - len(failed_items)

        if failed_items:
            logger.warning(
                f"Batch {batch_num} processed with failures: {len(failed_items)} failed items out of {len(docs)}."
            )
            logger.debug(f"Failed items in batch {batch_num}: {failed_items}")
        else:
            logger.info(
                f"Batch {batch_num} successfully processed with {success_count} documents."
            )
        return failed_items

    except TypesenseClientError as e:
        logger.error(f"Typesense client error in batch {batch_num}: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error processing batch {batch_num}: {e}")
        raise
