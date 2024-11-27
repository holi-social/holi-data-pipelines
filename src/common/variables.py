import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Union

from prefect.variables import Variable
from prefect_gcp.secret_manager import GcpSecret

# TODO maybe use pydantic.JsonType instead?
JsonType = Union[None, int, str, bool, List["JsonType"], Dict[str, "JsonType"]]

def _env_key_from_prefect_key(key: str) -> str:
    # environment variables shall be uppercase and delimited with underscores instead of hyphens
    return key.upper().replace("-", "_")

def _getenv_case_insensitive(key: str, default: str = None) -> JsonType:
    return os.getenv(_env_key_from_prefect_key(key), default)

def variable_value(key: str, default: Optional[JsonType] = None) -> JsonType:
    """
    Resolves the value for a given key by first attempting to retrieve it from
    Prefect variables. If not found, it falls back to searching the environment
    variables in a case-insensitive manner.

    Args:
        key: The key to look up. It needs to adhere to prefect variable naming (max 255 characters,
             lowercase alphanumeric characters ([a-z], [0-9]) or underscores (_)).
        default: The default value to return if the key is not found.

    Returns:
        The resolved value, parsed as JSON if applicable.

    """
    prefect_var = Variable.get(key)
    if not prefect_var:
        environment_var_or_default = _getenv_case_insensitive(key, default)
        if not environment_var_or_default:
            raise ValueError(
                f"Variable '{key}' can't be found, neither as prefect variable, nor as environment variable (key"
                f" '{_env_key_from_prefect_key(key)}')"
            )
        else:
            return json.loads(environment_var_or_default)
    else:
        return prefect_var


def secret_value(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Resolves the secret for a given key. If not found, it falls back to searching the environment
    variables in a case-insensitive manner.

    Args:
        key: The key to look up. It needs to adhere to prefect block naming (max 255 characters?,
             lowercase alphanumeric characters ([a-z], [0-9]) or dashes (-)).
        default: The default value to return if the key is not found.

    Returns:
        The resolved value as str
    """
    try:
        return GcpSecret.load(key).read_secret()
    except ValueError:
        environment_var_or_default = _getenv_case_insensitive(key, default)
        if not environment_var_or_default:
            raise ValueError(
                f"Secret '{key}' can't be found, neither as prefect variable, nor as environment variable (key"
                f" '{_env_key_from_prefect_key(key)}')"
            )
        else:
            return json.loads(environment_var_or_default)
