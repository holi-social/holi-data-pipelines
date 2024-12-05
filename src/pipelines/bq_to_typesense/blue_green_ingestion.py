import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

def setup_blue_green(typesense_client, schema: dict, alias_name="volunteering_search"):
    """Initial setup for blue/green collections and alias."""
    blue_collection = "volunteering_blue"
    green_collection = "volunteering_green"

    # Ensure both collections exist with the latest schema
    for collection_name in [blue_collection, green_collection]:
        try:
            typesense_client.collections[collection_name].delete()
        except Exception:
            pass  # Ignore if collection doesn't exist

        typesense_client.collections.create({
            "name": collection_name,
            "fields": schema["fields"],
        })
        logger.info(f"Collection {collection_name} created or updated.")

    # Ensure alias exists
    try:
        typesense_client.aliases[alias_name].retrieve()
    except Exception:
        # Create alias pointing to the blue collection initially
        typesense_client.aliases.create({
            "name": alias_name,
            "collection_name": blue_collection,
        })
        logger.info(f"Alias {alias_name} created and points to {blue_collection}.")

def ingest_data_with_tracking(typesense_client, documents, collection_name: str, batch_size=100):
    """Ingest data into the specified collection with progress tracking."""
    total_docs = len(documents)
    failed_docs = []
    success_count = 0

    def batches(iterable, size):
        """Yield successive n-sized batches from iterable."""
        for i in range(0, len(iterable), size):
            yield iterable[i:i + size]

    for batch_number, batch_docs in enumerate(batches(documents, batch_size), 1):
        try:
            response = typesense_client.collections[collection_name].documents.import_(batch_docs)
            failed = [doc for doc in response if not doc["success"]]
            failed_docs.extend(failed)
            success_count += len(batch_docs) - len(failed)
            logger.info(f"Batch {batch_number}: {len(batch_docs)} processed, {len(failed)} failed.")
        except Exception as e:
            logger.error(f"Batch {batch_number} failed: {str(e)}")

    logger.info(f"Ingestion completed for {collection_name}: {success_count}/{total_docs} successful.")
    return success_count, failed_docs

def validate_collection(typesense_client, collection_name: str, expected_count: int) -> bool:
    """Validate the new collection before switching the alias."""
    try:
        collection_info = typesense_client.collections[collection_name].retrieve()
        actual_count = collection_info["num_documents"]

        if actual_count < expected_count:
            logger.error(
                f"Validation failed for {collection_name}: Expected {expected_count}, found {actual_count}."
            )
            return False

        # Additional health checks, e.g., test a sample search
        sample_search = typesense_client.collections[collection_name].documents.search({
            "q": "*",
            "query_by": "title,description",
            "page": 1,
            "per_page": 1,
        })
        if not sample_search["hits"]:
            logger.error(f"Validation failed: No search results in {collection_name}.")
            return False

        logger.info(f"Validation passed for {collection_name}.")
        return True
    except Exception as e:
        logger.error(f"Validation failed for {collection_name}: {str(e)}")
        return False

def rollback_strategy(typesense_client, alias_name: str, current_collection: str):
    """Rollback to the current active collection."""
    try:
        typesense_client.aliases.upsert({
            "name": alias_name,
            "collection_name": current_collection,
        })
        logger.info(f"Alias rolled back to {current_collection}.")
    except Exception as e:
        logger.error(f"Rollback failed: {str(e)}")

def switch_alias_safe(typesense_client, alias_name: str, new_collection: str):
    """Switch alias to the new collection safely."""
    try:
        typesense_client.aliases.upsert({
            "name": alias_name,
            "collection_name": new_collection,
        })
        logger.info(f"Alias {alias_name} switched to {new_collection}.")
    except Exception as e:
        logger.error(f"Alias switch failed: {str(e)}")
        raise

def get_active_collection(typesense_client, alias_name: str) -> str:
    """Retrieve the current active collection for the alias."""
    alias_info = typesense_client.aliases[alias_name].retrieve()
    return alias_info["collection_name"]

def get_inactive_collection(active_collection: str) -> str:
    """Determine the inactive collection based on the active one."""
    return "volunteering_green" if active_collection == "volunteering_blue" else "volunteering_blue"

def blue_green_ingestion_pipeline(typesense_client, documents, alias_name="volunteering_search", schema=None):
    """Full blue/green ingestion pipeline."""
    if schema:
        setup_blue_green(typesense_client, schema, alias_name)

    current_collection = get_active_collection(typesense_client, alias_name)
    inactive_collection = get_inactive_collection(current_collection)

    logger.info(f"Current active collection: {current_collection}. Ingesting into {inactive_collection}.")

    # Step 1: Ingest data into the inactive collection
    success_count, failed_docs = ingest_data_with_tracking(typesense_client, documents, inactive_collection)
    if failed_docs:
        logger.error(f"Ingestion completed with {len(failed_docs)} failures. See logs for details.")
        rollback_strategy(typesense_client, alias_name, current_collection)
        return

    # Step 2: Validate the new collection
    if not validate_collection(typesense_client, inactive_collection, len(documents)):
        rollback_strategy(typesense_client, alias_name, current_collection)
        return

    # Step 3: Switch alias to the new collection
    try:
        switch_alias_safe(typesense_client, alias_name, inactive_collection)
        logger.info(f"Blue/green deployment successful. Alias now points to {inactive_collection}.")
    except Exception:
        rollback_strategy(typesense_client, alias_name, current_collection)
