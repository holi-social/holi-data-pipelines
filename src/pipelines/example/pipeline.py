from typing import cast

from prefect import task
from prefect.futures import PrefectFuture
from prefect.types import StrictVariableValue

from src.common.monitoring.alerting import alerting_flow


@task(log_prints=True)  # type: ignore[arg-type]
def say_hello(name: str) -> None:
    print(f"Hello, {name}!")  # noqa: T201


_CONFIG: dict[str, StrictVariableValue] = {"names": ["Marvin", "Trillian", "Ford"]}


@task()  # type: ignore[arg-type]
def the_answer() -> int:
    return 42


@task()  # type: ignore[arg-type]
def timed_out_answer() -> int:
    raise Exception("Deep thought not have enough time to complete calculation")


# TODO can we somehow avoid silencing mypy here?
@alerting_flow(context=_CONFIG)  # type: ignore[call-overload]
def hello_universe() -> None:
    for name in cast(list[str], _CONFIG["names"]):
        say_hello(name)

    timed_out_answer_future: PrefectFuture[int] = timed_out_answer.submit()
    timed_out_answer_result = timed_out_answer_future.wait()
    if not timed_out_answer_future.state.is_completed():
        print(  # noqa: T201
            f"Calculating the Answer to the Ultimate Question of Life, the Universe, and Everything: {timed_out_answer_result}"
        )
    else:
        print("I will never print the answer")  # noqa: T201

    the_answer_future: PrefectFuture[int] = the_answer.submit()
    the_answer_result = the_answer_future.result(raise_on_failure=False)
    print(f"The Answer to the Ultimate Question of Life, the Universe, and Everything: {the_answer_result}")  # noqa: T201


if __name__ == "__main__":
    hello_universe()
