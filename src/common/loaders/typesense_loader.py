import json
from typing import Iterable, Dict, Tuple, List
import prefect
from typesense.client import Client  # type: ignore

logger = prefect.logging.get_logger()

def upsert_documents_in_batches(
    client: Client, docs: Iterable[Dict[str, object]], batch_num: int, index_name: str
) -> Tuple[List[Dict[str, object]], int]:
    """
    Upsert a batch of documents into Typesense.

    Args:
        client (Client): The Typesense client instance used to interact with the Typesense API.
        docs (List[Dict[str, object]]): A list of dictionaries representing documents to be indexed.
                                        Each document is expected to have key-value pairs where keys are strings
                                        and values can be of any type (e.g., string, number, etc.).
        batch_num (int): The current batch number, used for logging and identifying the batch being processed.
        index_name (str): The name of the index to which the documents will be added or updated.

    Returns:
        List[Dict[str, object]]: A list of failed items (if any), where each item is a dictionary with information about the failure.

    """
    logger.debug(f"Upserting documents in batch {batch_num}")

    # see https://typesense.org/docs/27.1/api/documents.html#index-multiple-documents
    # with the default batch_size of 40 we saw 99% CPU usage over multiple hours during which search requests took
    # multiple seconds. a lower value should result in search requests being served with lower latency.
    server_side_batch_size = 5

    response = client.collections[index_name].documents.import_(
        documents=docs,
        params={"action": "upsert", "dirty_values": "coerce_or_reject"},
        batch_size=server_side_batch_size,
    )

    # Parse and filter results
    results = [json.loads(line) if isinstance(line, str) else line for line in response]
    failed_items = [item for item in results if not item.get("success", True)]
    success_count = len(results) - len(failed_items)

    return failed_items, success_count
