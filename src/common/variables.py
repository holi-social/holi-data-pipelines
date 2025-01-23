import json
import os
from json import JSONDecodeError
from typing import Optional, TypeVar, Type

from prefect.types import StrictVariableValue
from prefect.utilities.asyncutils import sync_compatible
from prefect.variables import Variable
from prefect_gcp.secret_manager import GcpSecret
from trycast import isassignable, checkcast  # type: ignore


def _env_key_from_prefect_key(key: str) -> str:
    # environment variables shall be uppercase and delimited with underscores instead of hyphens
    return key.upper().replace("-", "_")


def _getenv_case_insensitive(key: str, default: Optional[str] = None) -> str | None:
    return os.getenv(_env_key_from_prefect_key(key), default)


@sync_compatible
async def variable_value(key: str, default: StrictVariableValue = None) -> StrictVariableValue:
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
    prefect_var: StrictVariableValue = await Variable.aget(key)  # type: ignore[assignment]
    if prefect_var is not None:
        return prefect_var
    else:
        environment_var = _getenv_case_insensitive(key)
        if environment_var is not None:
            try:
                return json.loads(environment_var)
            except JSONDecodeError as decode_error:
                raise ValueError(
                    "Please specify environment variables in JSON format, "
                    "e.g. for a string, put quotes around it: `ENV_VAR='\"value\"'`."
                ) from decode_error
        elif default is not None:
            return default
        else:
            raise ValueError(
                f"Variable '{key}' can't be found, neither as prefect variable, nor as environment variable (key"
                f" '{_env_key_from_prefect_key(key)}')"
            )


T = TypeVar("T", bound=StrictVariableValue)


@sync_compatible
async def variable_value_as(return_type: Type[T], key: str, default: Optional[T] = None) -> T:
    value = await variable_value(key, default)
    if isassignable(value, return_type):
        return checkcast(return_type, value)
    else:
        raise TypeError(f"Expected {return_type}, got {type(value)}")


def update_variable(key: str, value: StrictVariableValue) -> None:
    Variable.set(name=key, value=value, overwrite=True)


@sync_compatible
async def secret_value(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Resolves the secret for a given key. If not found, it falls back to searching the environment
    variables in a case-insensitive manner.

    Args:
        key: The key to look up. It needs to adhere to prefect block naming (max 255 characters?,
             lowercase alphanumeric characters ([a-z], [0-9]) or dashes (-)).
        default: The default value to return if the key is not found.

    Returns:
        The resolved value as Optional[str]
    """
    try:
        return (await (await GcpSecret.load(key)).read_secret()).decode("utf-8")
    except ValueError:
        environment_var_or_default = _getenv_case_insensitive(key, default)
        if not environment_var_or_default:
            raise ValueError(
                f"Secret '{key}' can't be found, neither as prefect variable, nor as environment variable (key"
                f" '{_env_key_from_prefect_key(key)}')"
            ) from None
        else:
            return json.loads(environment_var_or_default)


@sync_compatible
async def get_secret_value(key: str, default: Optional[str] = None) -> str:
    """
    Resolves the secret for a given key. If not found, it falls back to searching the environment
    variables in a case-insensitive manner. If no value could be found anywhere, it fails with an AssertionError.

    Args:
        key: The key to look up. It needs to adhere to prefect block naming (max 255 characters?,
             lowercase alphanumeric characters ([a-z], [0-9]) or dashes (-)).
        default: The default value to return if the key is not found.

    Returns:
        The resolved value as Optional[str]

    :raises AssertionError
    """
    value = await secret_value(key, default)
    if value is None:
        raise ValueError(f"Got None value for secret '{key}'.")
    return value
