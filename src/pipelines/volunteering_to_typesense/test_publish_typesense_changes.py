from typing import List
from unittest.mock import MagicMock

import pytest

from src.common.events.VolunteeringDeletedEvent import VolunteeringDeletedEvent
from src.pipelines.volunteering_to_typesense.publish_typesense_changes import publish_volunteering_deletion


@pytest.fixture
def test_document_ids() -> List[str]:
    return ["123", "456"]


@pytest.fixture
def pubsub_client_mock() -> MagicMock:
    return MagicMock()


def test_publish_volunteering_deletion_success(pubsub_client_mock: MagicMock, test_document_ids: List[str]) -> None:
    result = publish_volunteering_deletion(pubsub_client_mock, test_document_ids)

    assert result is True
    assert pubsub_client_mock.publish.call_count == len(test_document_ids)
    for doc_id in test_document_ids:
        expected_event = VolunteeringDeletedEvent(doc_id)
        pubsub_client_mock.publish.assert_any_call(expected_event)


def test_publish_volunteering_deletion_failure(pubsub_client_mock: MagicMock, test_document_ids: List[str]) -> None:
    pubsub_client_mock.publish.side_effect = Exception("PubSub error")

    result = publish_volunteering_deletion(pubsub_client_mock, test_document_ids)

    assert result is False
