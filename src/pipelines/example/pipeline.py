from prefect import flow, task


@task(log_prints=True)
def say_hello(name: str):
    print(f"Hello, {name}!")


@flow
def hello_universe():
    for name in ["Marvin", "Trillian", "Ford"]:
        say_hello(name)


if __name__ == "__main__":
    hello_universe()
