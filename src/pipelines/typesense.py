from typing import cast
from src.common.variables import variable_value

schema_path: str = f"src/typesense/{cast(str, variable_value('prefect_environment'))}_all.json"
