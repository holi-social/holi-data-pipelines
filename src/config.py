import os

CONFIG = None


def get_config():
    global CONFIG

    if CONFIG is None:
        CONFIG = {
            "ENVIRONMENT": os.getenv("ENVIRONMENT"),
            "ENVIRONMENT_HOLI_API": os.getenv("ENVIRONMENT_HOLI_API"),
            "DRY_RUN": os.getenv("DRY_RUN") != "False",
            "DB_HOST": os.getenv("DB_HOST"),
            "DB_PORT": os.getenv("DB_PORT"),
            "DB_NAME": os.getenv("DB_NAME"),
            "DB_USERNAME": os.getenv("DB_USERNAME"),
            "DB_PASSWORD": os.getenv("DB_PASSWORD"),
            "GEOAPIFY_API_KEY": os.getenv("GEOAPIFY_API_KEY")
        }

    return CONFIG