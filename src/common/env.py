from prefect.variables import Variable


def is_production_environment() -> bool:
    return Variable.get("prefect_environment") == "production"
