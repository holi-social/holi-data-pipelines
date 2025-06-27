import pytest
from typing import Optional, cast

from shared.mappings.task_mappings import (
    map_task_location_type_to_typesense_location_type,
    map_task_regularity_to_commitment_label,
    OkunaTaskLocationType,
    OkunaTaskRegularity,
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
            ("UNKNOWN", ValueError()),
        ],
    )
    def test_task_mapping_location_type(self, actual: Optional[str], expected: Optional[str | ValueError]) -> None:
        if type(expected) is ValueError:
            with pytest.raises(ValueError):
                map_task_location_type_to_typesense_location_type(cast(OkunaTaskLocationType, actual))
        else:
            mapped = map_task_location_type_to_typesense_location_type(cast(OkunaTaskLocationType, actual))
            assert mapped == expected

    @pytest.mark.parametrize(
        "actual,expected",
        [
            ("ONE_TIME", "one-time"),
            ("ONCE_WEEK", "recurring"),
            ("ONCE_MONTH", "recurring"),
            ("REGULARLY", "recurring"),
            ("SEVERAL_TIMES_WEEK", "recurring"),
            ("PROJECT_BASED", "negotiable"),
            ("NEGOTIABLE", "negotiable"),
            (None, None),
            ("", None),
            ("UNKNOWN", ValueError()),
        ],
    )
    def test_task_mapping_commitment_label(self, actual: Optional[str], expected: Optional[str | ValueError]) -> None:
        if type(expected) is ValueError:
            with pytest.raises(ValueError):
                map_task_regularity_to_commitment_label(cast(OkunaTaskRegularity, actual))
        else:
            mapped = map_task_regularity_to_commitment_label(cast(OkunaTaskRegularity, actual))
            assert mapped == expected
