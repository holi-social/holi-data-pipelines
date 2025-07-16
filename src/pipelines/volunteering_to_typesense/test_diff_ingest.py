from typing import Any
from datetime import datetime, date, timedelta, timezone
from unittest.mock import patch

from pytest import fixture

from src.common.pubsub_client import PubSubClient
from src.pipelines.volunteering_to_typesense.tasks import diff_ingest, TypesenseDocId, ChangedRecords
from src.common.typesense_client import TypesenseClient
from src.pipelines.volunteering_to_typesense.test_typesense_transformer import _row


@fixture
def table_name() -> str:
    return "test_table_name"


@fixture
def typesense_collection_name() -> str:
    return "test_collection"


@fixture
def latest_partition_timestamp() -> date:
    return date.today()


@patch("src.common.typesense_client.TypesenseClient")
@patch("src.common.pubsub_client.PubSubClient")
@patch("src.pipelines.volunteering_to_typesense.tasks.extract_changed_records")
@patch("src.pipelines.volunteering_to_typesense.tasks.upsert_typesense_documents", return_value=True)
@patch("src.pipelines.volunteering_to_typesense.tasks.delete_typesense_documents", return_value=True)
@patch("src.pipelines.volunteering_to_typesense.tasks.publish_volunteering_deletion", return_value=True)
def test_diff_ingest(
    mock_publish_volunteering_deletion: Any,
    mock_delete_typesense_documents: Any,
    mock_upsert_typesense_documents: Any,
    mock_extract_changed_records: Any,
    mock_pubsub_client: PubSubClient,
    mock_typesense_client: TypesenseClient,
    table_name: str,
    latest_partition_timestamp: date,
    typesense_collection_name: str,
) -> None:
    changed_records = [({"change_type": "new_or_updated"} | _row()) for _ in range(3)]
    deleted_records = [({"change_type": "deleted"} | _row()) for _ in range(3)]

    mock_extract_changed_records.return_value = ChangedRecords(
        new_or_updated=changed_records,
        deleted=deleted_records,
    )

    # WHEN calling diff_ingest with a timestamp from the previous day
    last_complete_ingestion_timestamp: datetime = datetime.now(timezone.utc) - timedelta(days=1)
    (deletion_succeeded, upsert_succeeded, processed_items_count) = diff_ingest.fn(
        table_name,
        last_complete_ingestion_timestamp,
        latest_partition_timestamp,
        mock_typesense_client,
        mock_pubsub_client,
        typesense_collection_name,
    )

    # THEN diff will be calculated between current and previous day
    mock_extract_changed_records.assert_called_with(
        table_name,
        last_complete_ingestion_timestamp.date(),
        latest_partition_timestamp,
    )

    # THEN updated docs will be updated in Typesense
    args, kwargs = mock_upsert_typesense_documents.call_args
    assert args[2] == typesense_collection_name
    assert len(args[3]) == len(changed_records)

    # THEN deleted docs will be deleted from Typesense
    mock_delete_typesense_documents.assert_called_with(
        mock_typesense_client, typesense_collection_name, [TypesenseDocId(doc["id"]) for doc in deleted_records]
    )

    # THEN deletion events are published to Pubsub
    mock_publish_volunteering_deletion.assert_called_with(
        mock_pubsub_client, [TypesenseDocId(doc["id"]) for doc in deleted_records]
    )

    # THEN return values match expected values after successful execution
    assert deletion_succeeded
    assert upsert_succeeded
    assert processed_items_count == len(changed_records) + len(deleted_records)


@patch("src.common.typesense_client.TypesenseClient")
@patch("src.common.pubsub_client.PubSubClient")
@patch("src.pipelines.volunteering_to_typesense.tasks.extract_changed_records")
@patch("src.pipelines.volunteering_to_typesense.tasks.upsert_typesense_documents")
@patch("src.pipelines.volunteering_to_typesense.tasks.delete_typesense_documents")
@patch("src.pipelines.volunteering_to_typesense.tasks.publish_volunteering_deletion")
def test_diff_ingest_no_changes(
    mock_publish_volunteering_deletion: Any,
    mock_delete_typesense_documents: Any,
    mock_upsert_typesense_documents: Any,
    mock_extract_changed_records: Any,
    mock_pubsub_client: PubSubClient,
    mock_typesense_client: TypesenseClient,
    table_name: str,
    latest_partition_timestamp: date,
    typesense_collection_name: str,
) -> None:
    # GIVEN no changed and no delete records
    mock_extract_changed_records.return_value = ChangedRecords(new_or_updated=[], deleted=[])

    # WHEN calling diff_ingest with a timestamp from the previous day
    last_complete_ingestion_timestamp: datetime = datetime.now(timezone.utc) - timedelta(days=1)
    (deletion_succeeded, upsert_succeeded, processed_items_count) = diff_ingest.fn(
        table_name,
        last_complete_ingestion_timestamp,
        latest_partition_timestamp,
        mock_typesense_client,
        mock_pubsub_client,
        typesense_collection_name,
    )

    # THEN no documents were updated or deleted
    assert mock_upsert_typesense_documents.was_not_called()
    assert mock_delete_typesense_documents.was_not_called()

    # THEN no deletion events are published to Pubsub
    assert mock_publish_volunteering_deletion.was_not_called()

    # THEN return values match expected values after successful execution
    assert deletion_succeeded
    assert upsert_succeeded
    assert processed_items_count == 0
