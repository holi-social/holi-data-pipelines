import logging
from logging import Logger, LoggerAdapter
from typing import Union

from prefect.context import get_run_context, TaskRunContext, FlowRunContext
import prefect.logging


def __running_on_prefect() -> bool:
    context = get_run_context()
    return isinstance(context, FlowRunContext) or isinstance(context, TaskRunContext)


# ruff: noqa: N999 false-positive
class PipelineLogger(Logger):
    _sdt_logger: Logger
    _prefect_logger: Logger | LoggerAdapter | None  # type: ignore

    def __init__(self, logger_name: str, prefect_level_threshold: Union[str, int] = "INFO"):
        """
        @param logger_name: name of the logger
        @param prefect_level_threshold: threshold of the level from which logs also go to prefect
        """
        self._std_logger = logging.getLogger(logger_name)

        # Prefect loggers can only be instantiated if called in flow or task context
        self._prefect_logger = prefect.logging.get_run_logger() if __running_on_prefect() else None
        if self._prefect_logger is not None:
            self._prefect_logger.setLevel(prefect_level_threshold)

        super().__init__(logger_name)

    def debug(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.debug(msg, *args, **kwargs)
        if self._prefect_logger is not None:
            self._prefect_logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.info(msg, *args, **kwargs)
        if self._prefect_logger is not None:
            self._prefect_logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.warning(msg, *args, **kwargs)
        if self._prefect_logger is not None:
            self._prefect_logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.error(msg, *args, **kwargs)
        if self._prefect_logger is not None:
            self._prefect_logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.critical(msg, *args, **kwargs)
        if self._prefect_logger is not None:
            self._prefect_logger.critical(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.exception(msg, *args, **kwargs)
        if self._prefect_logger is not None:
            self._prefect_logger.exception(msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.log(level, msg, *args, **kwargs)
        if self._prefect_logger is not None:
            self._prefect_logger.log(level, msg, *args, **kwargs)
