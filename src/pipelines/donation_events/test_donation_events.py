import unittest
from unittest.mock import call, patch, MagicMock
from datetime import date

from shared.events import Donation, DonationCreatedEvent, DonationUpdatedEvent, DonationDeletedEvent
from src.pipelines.donation_events.donation_events import (
    extract_and_publish_donation_change_events,
    donation_events_flow,
)


test_data_from_bigquery_mock = [
    {"change_type": "created", "id": "1"},
    {"change_type": "created", "id": "2"},
    {"change_type": "updated", "id": "3"},
    {"change_type": "updated", "id": "4"},
    {"change_type": "updated", "id": "5"},
    {"change_type": "deleted", "id": "6"},
    {"change_type": "deleted", "id": "7"},
]


old_partition_date = date(2025, 8, 19)
new_partition_date = date(2025, 8, 20)


class TestDonationEventsFlow(unittest.TestCase):
    @patch("src.pipelines.donation_events.donation_events.logger")
    @patch("src.common.pubsub_client.PubSubClient")
    @patch("src.pipelines.donation_events.donation_events.donations_betterplace_raw", return_value="donations")
    @patch(
        "src.pipelines.donation_events.donation_events.extract_data_from_bigquery",
        return_value=test_data_from_bigquery_mock,
    )
    def test_events_are_published(
        self,
        extract_data_from_bigquery_mock: MagicMock,
        donations_betterplace_raw_mock: MagicMock,
        pubsub_client_mock: MagicMock,
        logger_mock: MagicMock,
    ) -> None:
        expected_calls = [
            call(DonationCreatedEvent(donation=Donation(id=1)), ordering_key="donation_1"),
            call(DonationCreatedEvent(donation=Donation(id=2)), ordering_key="donation_2"),
            call(DonationUpdatedEvent(donation=Donation(id=3)), ordering_key="donation_3"),
            call(DonationUpdatedEvent(donation=Donation(id=4)), ordering_key="donation_4"),
            call(DonationUpdatedEvent(donation=Donation(id=5)), ordering_key="donation_5"),
            call(DonationDeletedEvent(donation=Donation(id=6)), ordering_key="donation_6"),
            call(DonationDeletedEvent(donation=Donation(id=7)), ordering_key="donation_7"),
        ]

        extract_and_publish_donation_change_events(pubsub_client_mock, old_partition_date, new_partition_date)

        pubsub_client_mock.publish.assert_has_calls(expected_calls, any_order=True)

    @patch("src.pipelines.donation_events.donation_events.logger")
    @patch("src.common.pubsub_client.PubSubClient")
    @patch("src.pipelines.donation_events.donation_events.donations_betterplace_raw", return_value="donations")
    @patch(
        "src.pipelines.donation_events.donation_events.extract_data_from_bigquery",
        return_value=test_data_from_bigquery_mock,
    )
    def test_logs_stats_about_changes(
        self,
        extract_data_from_bigquery_mock: MagicMock,
        donations_betterplace_raw_mock: MagicMock,
        pubsub_client_mock: MagicMock,
        logger_mock: MagicMock,
    ) -> None:
        extract_and_publish_donation_change_events(pubsub_client_mock, old_partition_date, new_partition_date)

        logger_mock.info.assert_called_with("7 total changes, 2 creations, 3 updates, 2 deletions")

    @patch("src.pipelines.donation_events.donation_events.logger")
    @patch("src.common.pubsub_client.PubSubClient")
    @patch("src.pipelines.donation_events.donation_events.donations_betterplace_raw", return_value="donations")
    @patch(
        "src.pipelines.donation_events.donation_events.extract_data_from_bigquery",
        return_value=[{"change_type": "unknown", "id": "1"}],
    )
    def test_throws_exception_for_unknown_change_types(
        self,
        extract_data_from_bigquery_mock: MagicMock,
        donations_betterplace_raw_mock: MagicMock,
        pubsub_client_mock: MagicMock,
        logger_mock: MagicMock,
    ) -> None:
        with self.assertRaises(Exception) as ctx:
            extract_and_publish_donation_change_events(pubsub_client_mock, old_partition_date, new_partition_date)
        self.assertEqual("Unknown change type 'unknown'", str(ctx.exception))
        pubsub_client_mock.publish.assert_not_called()

    @patch("src.pipelines.donation_events.donation_events.variable_value_as", return_value="donations_topic")
    @patch("src.pipelines.donation_events.donation_events.PubSubClient")
    @patch("src.pipelines.donation_events.donation_events.donations_betterplace_raw", return_value="donations")
    @patch(
        "src.pipelines.donation_events.donation_events.extract_data_from_bigquery",
        return_value=[],
    )
    def test_returns_completed_state(
        self,
        extract_data_from_bigquery_mock: MagicMock,
        donations_betterplace_raw_mock: MagicMock,
        pubsub_client_mock: MagicMock,
        variable_value_as_mock: MagicMock,
    ) -> None:
        result = donation_events_flow.fn(old_partition_date, new_partition_date)

        self.assertEqual("COMPLETED", result.type)
