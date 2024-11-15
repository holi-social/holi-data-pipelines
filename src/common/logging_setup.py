import json
import logging
import os
import sys
from logging import config as logging_config
import structlog
import traceback

ENVIRONMENT_ID = os.getenv("ENVIRONMENT") # staging, production
SERVICE_NAME = "holi-data-classification"
TEAM_NAME = "Data&Matching"
LABELS = {"environment": ENVIRONMENT_ID, "service": SERVICE_NAME, "team": TEAM_NAME}

gcloud_severities = {
        "critical": "CRITICAL",
        "exception": "ERROR",
        "error": "ERROR",
        "warn": "WARNING",
        "warning": "WARNING",
        "info": "INFO",
        "debug": "DEBUG",
        "notset": "DEFAULT",
}

_shared_processors = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.UnicodeDecoder(),
]

def holi_gcloud_serializer(event_dict, **kwargs):
    # - Use Google fields for severity & other
    # - Add static applicationlevel fields (service & environment)
    if "exc_info" in event_dict:
        event_dict["traceback"] = "".join(
            traceback.format_exception(event_dict["exc_info"][0], event_dict["exc_info"][1], event_dict["exc_info"][2])
        )
    event_dict["message"] = event_dict.pop("event")
    event_dict["severity"] = gcloud_severities.get(event_dict.pop("level"))
    event_dict["logging.googleapis.com/labels"] = LABELS
    return json.dumps(event_dict, **kwargs)


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json_formatter": {
            "()": structlog.stdlib.ProcessorFormatter,
            "processor": structlog.processors.JSONRenderer(serializer=holi_gcloud_serializer),
            "foreign_pre_chain": _shared_processors,
        },
        "console_formatter": {
            "()": structlog.stdlib.ProcessorFormatter,
            "processor": structlog.dev.ConsoleRenderer(),
            "foreign_pre_chain": _shared_processors,
        },
    },
    "handlers": {
        "json": {
            "class": "logging.StreamHandler",
            "formatter": "json_formatter",
        },
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "console_formatter",
        },
    },
    "loggers": {
        "uvicorn.access": {
            "level": "WARNING",
        },
        # "django.db.backends": {
        #     "level": "DEBUG",
        # },
    },
    "root": {
        "level": "INFO",
        "handlers": ["console" if (sys.stdout.isatty() or "PYCHARM_HOSTED" in os.environ) else "json"],
    },
}

def initialize_logging():
    logging_config.dictConfig(LOGGING_CONFIG)

    structlog.configure(
        processors=[
            *_shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return LOGGING_CONFIG

