from typing import cast
from src.common.variables import variable_value

volunteering_voltastics_classified_enriched_partitioned: str = cast(
    str, variable_value("volunteering_voltastics_raw_deduped")
)
