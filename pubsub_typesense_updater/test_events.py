import unittest

from pubsub_typesense_updater.test_data import (
    create_test_cloud_event,
    test_cloud_event_donation_created,
    test_cloud_event_donation_deleted,
    test_cloud_event_donation_updated,
    test_event_donation_created,
    test_event_donation_deleted,
    test_event_donation_updated,
)

from pubsub_typesense_updater.events import (
    parse_donation_created_event,
    parse_donation_deleted_event,
    parse_donation_updated_event,
)
from pubsub_typesense_updater.test_enrich_donations import test_donation
from pubsub_typesense_updater.test_enrichments import valid_topics_embedding
from shared.events import DonationCreatedEvent


class TestEvents(unittest.TestCase):
    def test_should_parse_donation_created_event(self):
        actual_event: DonationCreatedEvent = parse_donation_created_event(test_cloud_event_donation_created)
        self.assertEqual(actual_event, test_event_donation_created)

    def test_should_fail_parsing_donation_created_event_for_wrong_type(self):
        cloud_event = create_test_cloud_event(event_type="unknown", event_version="1.0.0", data=test_donation)

        with self.assertRaises(Exception) as ctx:
            parse_donation_created_event(cloud_event)
        self.assertEqual("Expected event of type DonationCreated, got unknown", str(ctx.exception))

    def test_should_fail_parsing_donation_created_event_for_wrong_version(self):
        cloud_event = create_test_cloud_event(event_type="DonationCreated", event_version="0.1.0", data=test_donation)

        with self.assertRaises(Exception) as ctx:
            parse_donation_created_event(cloud_event)
        self.assertEqual("Unexpected event version: min=1.0.0, max_excl=2.0.0, actual=0.1.0", str(ctx.exception))

    def test_should_fail_parsing_donation_created_event_for_invalid_data(self):
        cloud_event = create_test_cloud_event(
            event_type="DonationCreated", event_version="1.0.0", data=valid_topics_embedding
        )
        with self.assertRaises(Exception) as ctx:
            parse_donation_created_event(cloud_event)
        self.assertIn("1 validation error for Donation", str(ctx.exception))

    def test_should_parse_donation_updated_event(self):
        actual_event = parse_donation_updated_event(test_cloud_event_donation_updated)
        self.assertEqual(actual_event, test_event_donation_updated)

    def test_should_fail_parsing_donation_updated_event_for_wrong_type(self):
        cloud_event = create_test_cloud_event(event_type="unknown", event_version="1.0.0", data=test_donation)

        with self.assertRaises(Exception) as ctx:
            parse_donation_updated_event(cloud_event)
        self.assertEqual("Expected event of type DonationUpdated, got unknown", str(ctx.exception))

    def test_should_fail_parsing_donation_updated_event_for_wrong_version(self):
        cloud_event = create_test_cloud_event(event_type="DonationUpdated", event_version="0.1.0", data=test_donation)

        with self.assertRaises(Exception) as ctx:
            parse_donation_updated_event(cloud_event)
        self.assertEqual("Unexpected event version: min=1.0.0, max_excl=2.0.0, actual=0.1.0", str(ctx.exception))

    def test_should_fail_parsing_donation_updated_event_for_invalid_data(self):
        cloud_event = create_test_cloud_event(
            event_type="DonationUpdated", event_version="1.0.0", data=valid_topics_embedding
        )
        with self.assertRaises(Exception) as ctx:
            parse_donation_updated_event(cloud_event)
        self.assertIn("1 validation error for Donation", str(ctx.exception))

    def test_should_parse_donation_deleted_event(self):
        actual_event = parse_donation_deleted_event(test_cloud_event_donation_deleted)
        self.assertEqual(actual_event, test_event_donation_deleted)

    def test_should_fail_parsing_donation_deleted_event_for_wrong_type(self):
        cloud_event = create_test_cloud_event(event_type="unknown", event_version="1.0.0", data=test_donation)

        with self.assertRaises(Exception) as ctx:
            parse_donation_deleted_event(cloud_event)
        self.assertEqual("Expected event of type DonationDeleted, got unknown", str(ctx.exception))

    def test_should_fail_parsing_donation_deleted_event_for_wrong_version(self):
        cloud_event = create_test_cloud_event(event_type="DonationDeleted", event_version="0.1.0", data=test_donation)

        with self.assertRaises(Exception) as ctx:
            parse_donation_deleted_event(cloud_event)
        self.assertEqual("Unexpected event version: min=1.0.0, max_excl=2.0.0, actual=0.1.0", str(ctx.exception))

    def test_should_fail_parsing_donation_deleted_event_for_invalid_data(self):
        cloud_event = create_test_cloud_event(
            event_type="DonationDeleted", event_version="1.0.0", data=valid_topics_embedding
        )
        with self.assertRaises(Exception) as ctx:
            parse_donation_deleted_event(cloud_event)
        self.assertIn("1 validation error for Donation", str(ctx.exception))
