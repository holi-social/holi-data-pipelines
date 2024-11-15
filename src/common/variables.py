import json
from prefect.variables import Variable

JsonType = Union[None, int, str, bool, List[JsonType], Dict[str, JsonType]]


def resolve(key: str, default: Optional[JsonType] = None) -> JsonType:
    """
    Resolves the value for a given key by first attempting to retrieve it from
    Prefect variables. If not found, it falls back to searching the environment
    variables in a case-insensitive manner.

    Args:
        key: The key to look up. It needs to adhere to prefect variable naming (max 255 characters, lowercase alphanumeric characters ([a-z], [0-9]) or underscores (_)).
        default: The default value to return if the key is not found.

    Returns:
        The resolved value, parsed as JSON if applicable.
    """

    def getenv_case_insensitive(key: str, default: str = None) -> JsonType:
        for env_key, value in os.environ.items():
            if env_key.lower() == key.lower():
                return value
        return default

    return json.loads(Variable.get(key) or getenv_case_insensitive(key, default))
