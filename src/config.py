import os

CONFIG = None

def get_config():
    global CONFIG

    if CONFIG is None:
        CONFIG = {
            "ENVIRONMENT": os.getenv("ENVIRONMENT"),
            "ENVIRONMENT_HOLI_API": os.getenv("ENVIRONMENT_HOLI_API"),
            "PROJECT_ID": os.getenv("PROJECT_ID"),
            "TYPESENSE_API_KEY": os.getenv("TYPESENSE_API_KEY"),
            "TYPESENSE_HOST": os.getenv("TYPESENSE_HOST"),
            "TYPESENSE_PORT": os.getenv("TYPESENSE_PORT"),
            "TYPESENSE_PROTOCOL": os.getenv("TYPESENSE_PROTOCOL"),
        }

    return CONFIG