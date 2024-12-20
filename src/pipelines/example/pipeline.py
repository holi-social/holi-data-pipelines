from typing import cast

from prefect import flow, task
import functools

from prefect.types import StrictVariableValue

from src.common.monitoring.alerting import alert


@task(log_prints=True)
def say_hello(name: str) -> None:
    print(f"Hello, {name}!")  # noqa: T201


_CONFIG: dict[str, StrictVariableValue] = {"names": ["Marvin", "Trillian", "Ford"]}


@flow(
    on_failure=[functools.partial(alert, _CONFIG)],
    on_crashed=[functools.partial(alert, _CONFIG)],
    on_cancellation=[functools.partial(alert, _CONFIG)],
)
def hello_universe() -> None:
    for name in cast(list[str], _CONFIG["names"]):
        say_hello(name)


if __name__ == "__main__":
    hello_universe()
