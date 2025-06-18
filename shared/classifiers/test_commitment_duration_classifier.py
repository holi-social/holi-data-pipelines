import pytest

from shared.classifiers.commitment_duration_classifier import parse_commitment_duration

VALID_VALUES = [
    "MINUTES_10to15",
    "MINUTES_15to30",
    "MINUTES_30to60",
    "HOURS_1to2",
    "HOURS_2to4",
    "HOURS_4to6",
    "DAYS_1",
    "DAYS_2",
    "DAYS_3_OR_MORE",
    "INDETERMINATE",
]


@pytest.mark.parametrize("input", VALID_VALUES)
def test_parse_commitment_duration_valid_values(input: str) -> None:
    parse_commitment_duration(input)


@pytest.mark.parametrize("input", [None, "", "INVALID"])
def test_parse_commitment_duration_invalid_values(input: str) -> None:
    with pytest.raises(ValueError):
        parse_commitment_duration(input)
