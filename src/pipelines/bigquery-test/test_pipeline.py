from prefect.testing.utilities import prefect_test_harness

from pipeline import mark_it_down, get_number_of_volunteering_opportunities


def test_markdown():
    with prefect_test_harness():
        mark_it_down('2024-03-26', 43210)


def test_bq():
    with prefect_test_harness():
        get_number_of_volunteering_opportunities('2024-03-26')
