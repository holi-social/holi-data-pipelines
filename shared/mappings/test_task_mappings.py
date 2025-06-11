import pytest
from typing import Optional

from shared.mappings.task_mappings import (
    map_task_location_type_to_typesense_location_type,
    map_task_regularity_to_commitment_label,
)


class TestTaskMappings:
    @pytest.mark.parametrize(
        "actual,expected",
        [
            ("HYBRID", "remote"),
            ("REMOTE", "remote"),
            ("ONSITE", "onsite"),
            (None, None),
            ("", None),
            ("UNKNOWN", None),
        ],
    )
    def test_task_mapping_location_type(self, actual: Optional[str], expected: Optional[str]) -> None:
        mapped = map_task_location_type_to_typesense_location_type(actual)
        assert mapped == expected

    @pytest.mark.parametrize(
        "actual,expected",
        [
            ("ONE_TIME", "one-time"),
            ("ONCE_WEEK", "recurring"),
            ("ONCE_MONTH", "recurring"),
            ("REGULARLY", "recurring"),
            ("PROJECT_BASED", "negotiable"),
            ("NEGOTIABLE", "negotiable"),
            (None, None),
            ("", None),
            ("UNKNOWN", None),
        ],
    )
    def test_task_mapping_commitment_label(self, actual: Optional[str], expected: Optional[str]) -> None:
        mapped = map_task_regularity_to_commitment_label(actual)
        assert mapped == expected
