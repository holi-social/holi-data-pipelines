import itertools
import json
from logging import Logger
from typing import Iterable, Dict, Tuple, List, Any, Generator
from urllib.parse import quote

import prefect
from prefect import task
from typesense.client import Client

from src.common.typesense_client import TypesenseClient


def batches[T](iterable: Iterable[T], batch_size: int = 1) -> Generator[Tuple[Iterable[T], int], None, None]:
    """Generator function yields batches of the original iterable in sizes of batch_size"""
    batch_number: int = 1
    for batch in itertools.batched(iterable, batch_size, strict=False):
        yield batch, batch_number
        batch_number += 1


def upsert_documents_in_batches(
    client: Client, logger: Logger, docs: Iterable[Dict[str, object]], batch_num: int, collection_name: str
) -> Tuple[List[Dict[str, object]], int]:
    """
    Upsert a batch of documents into Typesense.

    Args:
        client (Client): The Typesense client instance used to interact with the Typesense API.
        logger (Logger): The logger instance
        docs (List[Dict[str, object]]): A list of dictionaries representing documents to be indexed.
                                        Each document is expected to have key-value pairs where keys are strings
                                        and values can be of any type (e.g., string, number, etc.).
        batch_num (int): The current batch number, used for logging and identifying the batch being processed.
        collection_name (str): The name of the index to which the documents will be added or updated.

    Returns:
        List[Dict[str, object]]: A list of failed items (if any), where each item is a dictionary with information about the failure.

    """
    logger.debug(f"Upserting documents in batch {batch_num}")

    # see https://typesense.org/docs/27.1/api/documents.html#index-multiple-documents
    # with the default batch_size of 40 we saw 99% CPU usage over multiple hours during which search requests took
    # multiple seconds. a lower value results in search requests being served with lower latency.
    batch_size = 3

    url_encoded_collection_name = quote(collection_name)
    response = client.collections[url_encoded_collection_name].documents.import_(
        documents=docs, params={"action": "upsert", "dirty_values": "coerce_or_reject"}, batch_size=batch_size
    )

    # Parse and filter results
    results = [json.loads(line) if isinstance(line, str) else line for line in response]
    failed_items = [item for item in results if not item.get("success", True)]
    success_count = len(results) - len(failed_items)

    return failed_items, success_count


@task(cache_policy=prefect.cache_policies.NONE)  # type: ignore[arg-type]
def upsert_typesense_documents(
    typesense_client: TypesenseClient,
    logger: Logger,
    typesense_collection_name: str,
    new_or_updated_documents: List[Dict[str, Any]],
) -> bool:
    logger.info(f'Upserting {len(new_or_updated_documents)} documents into collection "{typesense_collection_name}"')
    ingestion_succeeded = True
    for docs_batch, batch_count in batches(new_or_updated_documents, 100):
        failed_documents, success_count = upsert_documents_in_batches(
            client=typesense_client.client,
            logger=logger,
            docs=docs_batch,
            batch_num=batch_count,
            collection_name=typesense_collection_name,
        )
        if failed_documents:
            ingestion_succeeded = False
            logger.error(
                f"Batch {batch_count} processed with failures: {len(failed_documents)} failed items out of"
                f" {success_count + len(failed_documents)}. Failed items in batch {batch_count}: {failed_documents}"
            )
        else:
            logger.info(f"Batch {batch_count} successfully upserted {success_count} documents.")
            logger.debug(f"Batch {batch_count} upserted document IDs: {[doc['id'] for doc in docs_batch]}")
    return ingestion_succeeded
