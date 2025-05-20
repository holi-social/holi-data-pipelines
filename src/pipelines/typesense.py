from typing import cast
from src.common.variables import variable_value

# Schema path for typesense collections
schema_path: str = f"src/typesense/{cast(str, variable_value('prefect_environment'))}_all.json"

LAST_INGESTION_TIMESTAMP_KEY = "last_complete_ingestion_timestamp"

VOLUNTEERING_STATE_VARIABLE = "volunteering_to_typesense_state"
AKTION_MENSCH_STATE_VARIABLE = "aktion_mensch_to_typesense_state"
