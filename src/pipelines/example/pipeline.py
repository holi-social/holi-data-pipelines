from typing import cast

from prefect import task
from prefect.types import StrictVariableValue

from src.common.monitoring.alerting import alerting_flow


@task(log_prints=True)
def say_hello(name: str) -> None:
    print(f"Hello, {name}!")  # noqa: T201


_CONFIG: dict[str, StrictVariableValue] = {"names": ["Marvin", "Trillian", "Ford"]}


# TODO can we somehow avoid silencing mypy here?
@alerting_flow(context=_CONFIG)  # type: ignore
def hello_universe() -> None:
    for name in cast(list[str], _CONFIG["names"]):
        say_hello(name)


if __name__ == "__main__":
    hello_universe()
