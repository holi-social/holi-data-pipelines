import unittest
from unittest.mock import patch, MagicMock
from prefect.states import Completed, State
from datetime import date
from src.pipelines.donation_updates.donation_updates import donation_updates_flow


old_partition_date = date(2025, 8, 19)
new_partition_date = date(2025, 8, 20)


class TestDonationUpdatesFlow(unittest.TestCase):
    @patch(
        "src.pipelines.donation_updates.donation_updates.crawl_betterplace",
        return_value=[old_partition_date, new_partition_date],
    )
    @patch("src.pipelines.donation_updates.donation_updates.donation_events_flow")
    def test_returns_events_flow_result_after_successful_crawl(
        self, donation_events_flow_mock: MagicMock, crawl_betterplace_mock: MagicMock
    ) -> None:
        expected_result: State = Completed()
        donation_events_flow_mock.return_value = expected_result

        result = donation_updates_flow.fn()

        self.assertEqual(expected_result, result)

    @patch(
        "src.pipelines.donation_updates.donation_updates.crawl_betterplace",
        return_value=None,
    )
    @patch("src.pipelines.donation_updates.donation_updates.donation_events_flow")
    def test_stops_and_returns_failed_if_crawler_failed(
        self, donation_events_flow_mock: MagicMock, crawl_betterplace_mock: MagicMock
    ) -> None:
        result = donation_updates_flow.fn()

        self.assertEqual("FAILED", result.type)
        self.assertEqual("Crawler run was not successful", result.message)
        donation_events_flow_mock.assert_not_called()

    @patch("src.pipelines.donation_updates.donation_updates.logger")
    @patch(
        "src.pipelines.donation_updates.donation_updates.crawl_betterplace",
        return_value=[old_partition_date, new_partition_date],
    )
    @patch("src.pipelines.donation_updates.donation_updates.donation_events_flow")
    def test_calls_events_flow_with_correct_partition_dates(
        self, donation_events_flow_mock: MagicMock, crawl_betterplace_mock: MagicMock, logger_mock: MagicMock
    ) -> None:
        donation_updates_flow.fn()

        logger_mock.info.assert_called_with("Old partition date: 2025-08-19, new partition date: 2025-08-20")
        donation_events_flow_mock.assert_called_with(old_partition_date, new_partition_date)
