import json
import os
from json import JSONDecodeError
from typing import Optional, TypeVar, Type, cast

from prefect.types import StrictVariableValue
from prefect.variables import Variable
from prefect_gcp.secret_manager import GcpSecret
from trycast import isassignable, checkcast  # type: ignore


def _env_key_from_prefect_key(key: str) -> str:
    # environment variables shall be uppercase and delimited with underscores instead of hyphens
    return key.upper().replace("-", "_")


def _getenv_case_insensitive(key: str, default: Optional[str] = None) -> str | None:
    return os.getenv(_env_key_from_prefect_key(key), default)


def _get_environment_variable_parsed(key: str) -> StrictVariableValue | None:
    environment_var = _getenv_case_insensitive(key)
    if environment_var is not None:
        try:
            return json.loads(environment_var)
        except JSONDecodeError as decode_error:
            raise ValueError(
                "Please specify environment variables in JSON format, "
                "e.g. for a string, put quotes around it: `ENV_VAR='\"value\"'`."
            ) from decode_error
    return None


def _value_or_default_else_error(
    value_type: str, key: str, value: StrictVariableValue, default: StrictVariableValue
) -> StrictVariableValue:
    if value is not None:
        return value
    elif default is not None:
        return default
    else:
        raise ValueError(
            f"{value_type} '{key}' can't be found, neither as prefect variable, nor as environment variable (key"
            f" '{_env_key_from_prefect_key(key)}')"
        )


def _variable_or_default_else_error(
    key: str, value: StrictVariableValue, default: StrictVariableValue
) -> StrictVariableValue:
    return _value_or_default_else_error("Variable", key, value, default)


def _secret_or_default_else_error(key: str, value: StrictVariableValue, default: StrictVariableValue) -> str:
    return _typechecked_value_or_raise(str, _value_or_default_else_error("Secret", key, value, default))


async def avariable_value(key: str, default: StrictVariableValue = None) -> StrictVariableValue:
    """
    Resolves the value for a given key by first attempting to retrieve it from
    local environment variables in a case-insensitive manner. If not found, it
    falls back to searching prefect variables.

    :param key: The key to look up. It needs to adhere to prefect variable naming (max 255 characters,
    lowercase alphanumeric characters ([a-z], [0-9]) or underscores (_)).
    :param default: The default value to return if the key is not found.
    :return: The resolved value, parsed from JSON.
    """
    value = _get_environment_variable_parsed(key) or await Variable.aget(key)  # type: ignore[assignment]
    return _variable_or_default_else_error(key, value, default)


def variable_value(key: str, default: StrictVariableValue = None) -> StrictVariableValue:
    """
    Resolves the value for a given key by first attempting to retrieve it from
    local environment variables in a case-insensitive manner. If not found, it
    falls back to searching prefect variables.

    :param key: The key to look up. It needs to adhere to prefect variable naming (max 255 characters,
     lowercase alphanumeric characters ([a-z], [0-9]) or underscores (_)).
    :param default: The default value to return if the key is not found.
    :return: The resolved value, parsed from JSON.
    """
    # FIXME remove casting once https://github.com/PrefectHQ/prefect/issues/16825 has a resolution
    value = _get_environment_variable_parsed(key) or cast(StrictVariableValue, Variable.get(key))
    return _variable_or_default_else_error(key, value, default)


T = TypeVar("T", bound=StrictVariableValue)


def _typechecked_value_or_raise(return_type: Type[T], value: StrictVariableValue) -> T:
    if isassignable(value, return_type):
        return checkcast(return_type, value)
    else:
        raise TypeError(f"Expected {return_type}, got {type(value)}")


async def avariable_value_as(return_type: Type[T], key: str, default: Optional[T] = None) -> T:
    """
    Typesafelly resolves the value for a given key by first attempting to retrieve it from
    local environment variables in a case-insensitive manner. If not found, it
    falls back to searching prefect variables.

    :param return_type: The expected type
    :param key: The key to look up. It needs to adhere to prefect  variable naming (max 255 characters,
     lowercase alphanumeric characters ([a-z], [0-9]) or underscores (_)).
    :param default: The default value to return if the key is not found.
    :return: The resolved value, parsed from JSON.
    """
    return _typechecked_value_or_raise(return_type, await avariable_value(key, default))


def variable_value_as(return_type: Type[T], key: str, default: Optional[T] = None) -> T:
    """
    Typesafelly resolves the value for a given key by first attempting to retrieve it from
    local environment variables in a case-insensitive manner. If not found, it
    falls back to searching prefect variables.

    :param return_type: The expected type
    :param key: The key to look up. It needs to adhere to prefect  variable naming (max 255 characters,
     lowercase alphanumeric characters ([a-z], [0-9]) or underscores (_)).
    :param default: The default value to return if the key is not found.
    :return: The resolved value, parsed from JSON.
    """
    return _typechecked_value_or_raise(return_type, variable_value(key, default))


def update_variable(key: str, value: StrictVariableValue) -> None:
    """
    Updates the value for a given key in the prefect variables. If the key does not exist, it will be created.

    :param key: The key to update. It needs to adhere to prefect variable naming (max 255 characters,
     lowercase alphanumeric characters ([a-z], [0-9]) or underscores (_)).
    :param value: The value to set.
    :return: None
    """
    Variable.set(name=key, value=value, overwrite=True)


async def asecret_value(key: str, default: Optional[str] = None) -> str:
    """
    Resolves the secret for a given key by first attempting to retrieve it from
    local environment variables in a case-insensitive manner. If not found, it
    falls back to searching GCP Secrets.

    :param key: The key to look up. It needs to adhere to prefect block naming (max 255 characters?, lowercase
    alphanumeric characters ([a-z], [0-9]) or dashes (-)).
    :param default: The default value to return if the key is not found.
    :return: The resolved value
    """
    try:
        return _typechecked_value_or_raise(
            str,
            _get_environment_variable_parsed(key)
            or (await (await GcpSecret.aload(key)).read_secret()).decode("utf-8"),
        )
    except ValueError:
        # not found via environment variables or GcpSecret block
        return _secret_or_default_else_error(key, None, default)


def secret_value(key: str, default: Optional[str] = None) -> str:
    """
    Resolves the secret for a given key by first attempting to retrieve it from
    local environment variables in a case-insensitive manner. If not found, it
    falls back to searching GCP Secrets.

    :param key: The key to look up. It needs to adhere to prefect block naming (max 255 characters?, lowercase
    alphanumeric characters ([a-z], [0-9]) or dashes (-)).
    :param default: The default value to return if the key is not found.
    :return: The resolved value
    """
    try:
        return _typechecked_value_or_raise(
            str, _get_environment_variable_parsed(key) or GcpSecret.load(key).read_secret().decode("utf-8")
        )
    except ValueError:
        # not found via environment variables or GcpSecret block
        return _secret_or_default_else_error(key, None, default)
