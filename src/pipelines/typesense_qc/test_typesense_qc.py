import unittest
from unittest.mock import patch, MagicMock
from src.common.typesense_client import TypesenseClient
from typing import Optional, Callable

from src.pipelines.typesense_qc.typesense_qc import ConsistencyCheck, typesense_qc_flow


def create_ids(count: int) -> list[str]:
    return [str(i) for i in range(count)]


all_docs = create_ids(100)
all_docs_variant = create_ids(99) + ["101"]
few_missing = create_ids(99)
too_many_missing = create_ids(94)
few_too_many = create_ids(101)
too_many = create_ids(106)


class TestTypesenseConsistencyCheck(unittest.TestCase):
    def test_consistency_check_passes_when_all_id_sets_are_equal(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=all_docs,
            bq_ids=all_docs,
            ts_ids=all_docs,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertTrue(passes)

    def test_consistency_check_passes_when_not_too_many_docs_are_missing_in_ts(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=all_docs,
            bq_ids=all_docs,
            ts_ids=few_missing,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertTrue(passes)

    def test_consistency_check_fails_when_too_many_docs_are_missing_in_ts(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=all_docs,
            bq_ids=all_docs,
            ts_ids=too_many_missing,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertFalse(passes)

    def test_consistency_check_passes_when_there_are_not_too_many_additional_docs_in_ts(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=all_docs,
            bq_ids=all_docs,
            ts_ids=few_too_many,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertTrue(passes)

    def test_consistency_check_fails_when_there_are_too_many_additional_docs_in_ts(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=all_docs,
            bq_ids=all_docs,
            ts_ids=too_many,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertFalse(passes)

    def test_consistency_check_succeeds_when_there_only_few_bq_docs_are_missing(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=all_docs,
            bq_ids=few_missing,
            ts_ids=all_docs,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertTrue(passes)

    def test_consistency_check_fails_when_too_many_bq_docs_are_missing(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=all_docs,
            bq_ids=too_many_missing,
            ts_ids=all_docs,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertFalse(passes)

    def test_consistency_check_succeeds_when_there_are_not_too_many_additional_docs_in_bq(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=all_docs,
            bq_ids=few_too_many,
            ts_ids=all_docs,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertTrue(passes)

    def test_consistency_check_fails_when_there_too_many_docs_in_bq(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=all_docs,
            bq_ids=too_many,
            ts_ids=all_docs,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertFalse(passes)

    def test_consistency_check_fails_when_additional_ts_docs_are_not_equal_to_unprocessed_pg_docs(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=all_docs,
            bq_ids=few_missing,
            ts_ids=all_docs_variant,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertFalse(passes)

    def test_consistency_check_succeeds_when_there_are_not_too_many_unprocessed_docs_in_pg(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=few_too_many,
            bq_ids=all_docs,
            ts_ids=all_docs,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertTrue(passes)

    def test_consistency_check_fails_when_there_are_too_many_unprocessed_docs_in_pg(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=too_many,
            bq_ids=all_docs,
            ts_ids=all_docs,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertFalse(passes)

    def test_consistency_check_succeeds_when_there_are_not_too_many_unprocessed_deleted_docs_in_pg(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=few_missing,
            bq_ids=all_docs,
            ts_ids=all_docs,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertTrue(passes)

    def test_consistency_check_succeeds_when_there_are_too_many_unprocessed_deleted_docs_in_pg(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=too_many_missing,
            bq_ids=all_docs,
            ts_ids=all_docs,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertFalse(passes)

    def test_consistency_check_ignores_pg_if_pg_is_not_the_data_source(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=None,
            bq_ids=all_docs,
            ts_ids=all_docs,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertTrue(passes)

    def test_consistency_check_succeeds_if_combined_diff_with_bq_is_small_enough(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=few_missing,
            bq_ids=all_docs,
            ts_ids=few_too_many,
        )

        passes = consistency_check.passes_consistency_check()

        self.assertTrue(passes)

    def test_consistency_check_fails_if_combined_diff_with_bq_is_too_large(self) -> None:
        consistency_check = ConsistencyCheck(
            pg_ids=create_ids(98),
            bq_ids=all_docs,
            ts_ids=create_ids(102),
        )

        passes = consistency_check.passes_consistency_check()

        self.assertFalse(passes)


id_list = ["1", "2", "3"]
too_many_ids_missing_list = ["1"]


def mock_get_ts_ids(
    results: Optional[dict[str, list[str]]] = None,
) -> Callable[[TypesenseClient, str, str], list[str]]:
    def result_fn(ts_client: TypesenseClient, ts_collection: str, document_type: str) -> list[str]:
        return results[document_type] if results and document_type in results else id_list

    return result_fn


class TestTypesenseQCFlow(unittest.TestCase):
    @patch("src.pipelines.typesense_qc.typesense_qc.variable_value")
    @patch("src.pipelines.typesense_qc.typesense_qc.TypesenseClientConfig")
    @patch(
        "src.pipelines.typesense_qc.typesense_qc.get_ids_ts",
        side_effect=mock_get_ts_ids(),
    )
    @patch("src.pipelines.typesense_qc.typesense_qc.get_donation_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_volunteering_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_pg", return_value=id_list)
    def test_should_succeed_if_all_types_pass_consistency_check(
        self,
        get_profile_ids_pg_mock: MagicMock,
        get_profile_ids_bq_mock: MagicMock,
        get_space_ids_pg_mock: MagicMock,
        get_space_ids_bq_mock: MagicMock,
        get_task_ids_pg_mock: MagicMock,
        get_task_ids_bq_mock: MagicMock,
        get_volunteering_ids_bq_mock: MagicMock,
        get_donation_ids_bq_mock: MagicMock,
        get_ids_ts_mock: MagicMock,
        ts_client_config_mock: MagicMock,
        variable_value_mock: MagicMock,
    ) -> None:
        result = typesense_qc_flow.fn()

        self.assertEqual(result.type, "COMPLETED")

    @patch("src.pipelines.typesense_qc.typesense_qc.variable_value")
    @patch("src.pipelines.typesense_qc.typesense_qc.TypesenseClientConfig")
    @patch(
        "src.pipelines.typesense_qc.typesense_qc.get_ids_ts",
        side_effect=mock_get_ts_ids(),
    )
    @patch("src.pipelines.typesense_qc.typesense_qc.get_donation_ids_bq", return_value=too_many_ids_missing_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_volunteering_ids_bq", return_value=too_many_ids_missing_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_bq", return_value=too_many_ids_missing_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_bq", return_value=too_many_ids_missing_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_bq", return_value=too_many_ids_missing_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_pg", return_value=id_list)
    def test_should_fail_if_all_types_fail_consistency_check(
        self,
        get_profile_ids_pg_mock: MagicMock,
        get_profile_ids_bq_mock: MagicMock,
        get_space_ids_pg_mock: MagicMock,
        get_space_ids_bq_mock: MagicMock,
        get_task_ids_pg_mock: MagicMock,
        get_task_ids_bq_mock: MagicMock,
        get_volunteering_ids_bq_mock: MagicMock,
        get_donation_ids_bq_mock: MagicMock,
        get_ids_ts_mock: MagicMock,
        ts_client_config_mock: MagicMock,
        variable_value_mock: MagicMock,
    ) -> None:
        result = typesense_qc_flow.fn()

        self.assertEqual(result.type, "FAILED")
        self.assertIn("BigQuery and Typesense are inconsistent for document type profile", result.message or "")
        self.assertIn("BigQuery and Typesense are inconsistent for document type space", result.message or "")
        self.assertIn("BigQuery and Typesense are inconsistent for document type task", result.message or "")
        self.assertIn("BigQuery and Typesense are inconsistent for document type volunteering", result.message or "")
        self.assertIn("BigQuery and Typesense are inconsistent for document type donation", result.message or "")

    @patch("src.pipelines.typesense_qc.typesense_qc.variable_value")
    @patch("src.pipelines.typesense_qc.typesense_qc.TypesenseClientConfig")
    @patch(
        "src.pipelines.typesense_qc.typesense_qc.get_ids_ts",
        side_effect=mock_get_ts_ids(),
    )
    @patch("src.pipelines.typesense_qc.typesense_qc.get_donation_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_volunteering_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_bq", return_value=too_many_ids_missing_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_pg", return_value=id_list)
    def test_should_fail_if_profiles_fail_consistency_check(
        self,
        get_profile_ids_pg_mock: MagicMock,
        get_profile_ids_bq_mock: MagicMock,
        get_space_ids_pg_mock: MagicMock,
        get_space_ids_bq_mock: MagicMock,
        get_task_ids_pg_mock: MagicMock,
        get_task_ids_bq_mock: MagicMock,
        get_volunteering_ids_bq_mock: MagicMock,
        get_donation_ids_bq_mock: MagicMock,
        get_ids_ts_mock: MagicMock,
        ts_client_config_mock: MagicMock,
        variable_value_mock: MagicMock,
    ) -> None:
        result = typesense_qc_flow.fn()

        self.assertEqual(result.type, "FAILED")
        self.assertEqual(
            "BigQuery and Typesense are inconsistent for document type profile: {'pg_ids_not_in_bq': ['2', '3'], 'bq_ids_not_in_pg': [], 'bq_ids_not_in_ts': [], 'ts_ids_not_in_bq': ['2', '3']}",
            result.message,
        )

    @patch("src.pipelines.typesense_qc.typesense_qc.variable_value")
    @patch("src.pipelines.typesense_qc.typesense_qc.TypesenseClientConfig")
    @patch(
        "src.pipelines.typesense_qc.typesense_qc.get_ids_ts",
        side_effect=mock_get_ts_ids(),
    )
    @patch("src.pipelines.typesense_qc.typesense_qc.get_donation_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_volunteering_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_bq", return_value=too_many_ids_missing_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_pg", return_value=id_list)
    def test_should_fail_if_spaces_fail_consistency_check(
        self,
        get_profile_ids_pg_mock: MagicMock,
        get_profile_ids_bq_mock: MagicMock,
        get_space_ids_pg_mock: MagicMock,
        get_space_ids_bq_mock: MagicMock,
        get_task_ids_pg_mock: MagicMock,
        get_task_ids_bq_mock: MagicMock,
        get_volunteering_ids_bq_mock: MagicMock,
        get_donation_ids_bq_mock: MagicMock,
        get_ids_ts_mock: MagicMock,
        ts_client_config_mock: MagicMock,
        variable_value_mock: MagicMock,
    ) -> None:
        result = typesense_qc_flow.fn()

        self.assertEqual(result.type, "FAILED")
        self.assertEqual(
            "BigQuery and Typesense are inconsistent for document type space: {'pg_ids_not_in_bq': ['2', '3'], 'bq_ids_not_in_pg': [], 'bq_ids_not_in_ts': [], 'ts_ids_not_in_bq': ['2', '3']}",
            result.message,
        )

    @patch("src.pipelines.typesense_qc.typesense_qc.variable_value")
    @patch("src.pipelines.typesense_qc.typesense_qc.TypesenseClientConfig")
    @patch(
        "src.pipelines.typesense_qc.typesense_qc.get_ids_ts",
        side_effect=mock_get_ts_ids(),
    )
    @patch("src.pipelines.typesense_qc.typesense_qc.get_donation_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_volunteering_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_bq", return_value=too_many_ids_missing_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_pg", return_value=id_list)
    def test_should_fail_if_tasks_fail_consistency_check(
        self,
        get_profile_ids_pg_mock: MagicMock,
        get_profile_ids_bq_mock: MagicMock,
        get_space_ids_pg_mock: MagicMock,
        get_space_ids_bq_mock: MagicMock,
        get_task_ids_pg_mock: MagicMock,
        get_task_ids_bq_mock: MagicMock,
        get_volunteering_ids_bq_mock: MagicMock,
        get_donation_ids_bq_mock: MagicMock,
        get_ids_ts_mock: MagicMock,
        ts_client_config_mock: MagicMock,
        variable_value_mock: MagicMock,
    ) -> None:
        result = typesense_qc_flow.fn()

        self.assertEqual(result.type, "FAILED")
        self.assertEqual(
            "BigQuery and Typesense are inconsistent for document type task: {'pg_ids_not_in_bq': ['2', '3'], 'bq_ids_not_in_pg': [], 'bq_ids_not_in_ts': [], 'ts_ids_not_in_bq': ['2', '3']}",
            result.message,
        )

    @patch("src.pipelines.typesense_qc.typesense_qc.variable_value")
    @patch("src.pipelines.typesense_qc.typesense_qc.TypesenseClientConfig")
    @patch(
        "src.pipelines.typesense_qc.typesense_qc.get_ids_ts",
        side_effect=mock_get_ts_ids(),
    )
    @patch("src.pipelines.typesense_qc.typesense_qc.get_donation_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_volunteering_ids_bq", return_value=too_many_ids_missing_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_pg", return_value=id_list)
    def test_should_fail_if_volunteerings_fail_consistency_check(
        self,
        get_profile_ids_pg_mock: MagicMock,
        get_profile_ids_bq_mock: MagicMock,
        get_space_ids_pg_mock: MagicMock,
        get_space_ids_bq_mock: MagicMock,
        get_task_ids_pg_mock: MagicMock,
        get_task_ids_bq_mock: MagicMock,
        get_volunteering_ids_bq_mock: MagicMock,
        get_donation_ids_bq_mock: MagicMock,
        get_ids_ts_mock: MagicMock,
        ts_client_config_mock: MagicMock,
        variable_value_mock: MagicMock,
    ) -> None:
        result = typesense_qc_flow.fn()

        self.assertEqual(result.type, "FAILED")
        self.assertEqual(
            "BigQuery and Typesense are inconsistent for document type volunteering: {'pg_ids_not_in_bq': [], 'bq_ids_not_in_pg': [], 'bq_ids_not_in_ts': [], 'ts_ids_not_in_bq': ['2', '3']}",
            result.message,
        )

    @patch("src.pipelines.typesense_qc.typesense_qc.variable_value")
    @patch("src.pipelines.typesense_qc.typesense_qc.TypesenseClientConfig")
    @patch(
        "src.pipelines.typesense_qc.typesense_qc.get_ids_ts",
        side_effect=mock_get_ts_ids(),
    )
    @patch("src.pipelines.typesense_qc.typesense_qc.get_donation_ids_bq", return_value=too_many_ids_missing_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_volunteering_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_task_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_space_ids_pg", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_bq", return_value=id_list)
    @patch("src.pipelines.typesense_qc.typesense_qc.get_profile_ids_pg", return_value=id_list)
    def test_should_fail_if_donations_fail_consistency_check(
        self,
        get_profile_ids_pg_mock: MagicMock,
        get_profile_ids_bq_mock: MagicMock,
        get_space_ids_pg_mock: MagicMock,
        get_space_ids_bq_mock: MagicMock,
        get_task_ids_pg_mock: MagicMock,
        get_task_ids_bq_mock: MagicMock,
        get_volunteering_ids_bq_mock: MagicMock,
        get_donation_ids_bq_mock: MagicMock,
        get_ids_ts_mock: MagicMock,
        ts_client_config_mock: MagicMock,
        variable_value_mock: MagicMock,
    ) -> None:
        result = typesense_qc_flow.fn()

        self.assertEqual(result.type, "FAILED")
        self.assertEqual(
            "BigQuery and Typesense are inconsistent for document type donation: {'pg_ids_not_in_bq': [], 'bq_ids_not_in_pg': [], 'bq_ids_not_in_ts': [], 'ts_ids_not_in_bq': ['2', '3']}",
            result.message,
        )
