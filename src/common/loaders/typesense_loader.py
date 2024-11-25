import json
from typing import List, Dict
import prefect

logger = prefect.logging.get_logger()

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

    """
    # Import documents with upsert option
    # TODO: This works for initial sync but we need to figure out how to do this for later syncs
    response = client.collections[index_name].documents.import_(
        docs, {"action": "upsert", "dirty_values": "coerce_or_reject"}
    )

    # Parse and filter results
    results = [json.loads(line) if isinstance(line, str) else line for line in response]
    failed_items = [item for item in results if not item.get("success", True)]
    success_count = len(results) - len(failed_items)

    return failed_items, success_count
