import unittest
from unittest.mock import patch, MagicMock

from shared.events import Donation, DonationCreatedEvent
from src.common.pubsub_client import PubSubClient

test_event = DonationCreatedEvent(Donation(id=1))
expected_event_data = b'{"links":[],"profile_picture":null,"contact":null,"completed_need_count":null,"comments_count":null,"donor_count":null,"donations_count":null,"negative_opinions_count":null,"positive_opinions_count":null,"blog_post_count":null,"newsletter_subscriptions_count":null,"incomplete_need_count":null,"carrier":null,"carrier_url":null,"carrier_country_en":null,"id":1,"title":null,"title_en":null,"description":null,"description_en":null,"summary":null,"summary_en":null,"hash":null,"open_amount_in_cents":null,"donated_amount_in_cents":null,"tax_deductible":null,"donations_prohibited":null,"progress_percentage":null,"street":null,"city":null,"zip":null,"country":null,"country_en":null,"latitude":null,"longitude":null,"created_at":null,"updated_at":null,"content_updated_at":null,"activated_at":null,"completed_at":null,"closed_at":null,"active_matching_fund":null,"matching_events":[],"categories":[]}'

expected_topic_name = "projects/testProject/topics/test_topic"


class TestPubSubClient(unittest.TestCase):
    @patch("src.common.pubsub_client.variable_value_as", return_value="testProject")
    def test_should_initialize_correctly_with_message_ordering(self, variable_value_as_mock: MagicMock) -> None:
        client = PubSubClient(topic_name="test_topic", ordering_key="test_ordering_key")

        self.assertEqual(client.topic_name, expected_topic_name)
        self.assertEqual(client.ordering_key, "test_ordering_key")
        self.assertTrue(client.publisher.publisher_options.enable_message_ordering)

    @patch("src.common.pubsub_client.variable_value_as", return_value="testProject")
    def test_should_initialize_correctly_without_message_ordering(self, variable_value_as_mock: MagicMock) -> None:
        client = PubSubClient(topic_name="test_topic")

        self.assertEqual(client.topic_name, "projects/testProject/topics/test_topic")
        self.assertIsNone(client.ordering_key)
        self.assertFalse(client.publisher.publisher_options.enable_message_ordering)

    @patch("src.common.pubsub_client.pubsub_v1.PublisherClient")
    @patch("src.common.pubsub_client.variable_value_as", return_value="testProject")
    def test_should_publish_event_with_ordering_key(
        self, variable_value_as_mock: MagicMock, publisher_mock: MagicMock
    ) -> None:
        client = PubSubClient(topic_name="test_topic", ordering_key="test_ordering_key")

        client.publish(test_event, ordering_key="donation_1")

        client.publisher.publish.assert_called_with(
            expected_topic_name,
            data=expected_event_data,
            ordering_key="donation_1",
            eventType="DonationCreated",
            eventVersion="1.0.0",
        )

    @patch("src.common.pubsub_client.pubsub_v1.PublisherClient")
    @patch("src.common.pubsub_client.variable_value_as", return_value="testProject")
    def test_should_publish_event_with_fallback_ordering_key(
        self, variable_value_as_mock: MagicMock, publisher_mock: MagicMock
    ) -> None:
        client = PubSubClient(topic_name="test_topic", ordering_key="test_ordering_key")

        client.publish(event=test_event)

        client.publisher.publish.assert_called_with(
            expected_topic_name,
            data=expected_event_data,
            ordering_key="test_ordering_key",
            eventType="DonationCreated",
            eventVersion="1.0.0",
        )

    @patch("src.common.pubsub_client.pubsub_v1.PublisherClient")
    @patch("src.common.pubsub_client.variable_value_as", return_value="testProject")
    def test_should_publish_event_without_ordering_key(
        self, variable_value_as_mock: MagicMock, publisher_mock: MagicMock
    ) -> None:
        client = PubSubClient(topic_name="test_topic")

        client.publish(event=test_event)

        client.publisher.publish.assert_called_with(
            expected_topic_name,
            data=expected_event_data,
            ordering_key=None,
            eventType="DonationCreated",
            eventVersion="1.0.0",
        )
