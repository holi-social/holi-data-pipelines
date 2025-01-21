import logging
from logging import Logger
from typing import Any

import prefect.logging
from prefect.context import get_run_context, TaskRunContext, FlowRunContext
from prefect.exceptions import MissingContextError


# ruff: noqa: N999 false-positive
class PipelineLogger(Logger):
    _sdt_logger: Logger

    def __init__(self, logger_name: str):
        """
        @param logger_name: name of the logger
        @param prefect_level_threshold: threshold of the level from which logs also go to prefect
        """
        self._std_logger = logging.getLogger(logger_name)
        super().__init__(logger_name)

    @staticmethod
    def __running_on_prefect() -> bool:
        try:
            context = get_run_context()
        except MissingContextError:
            return False
        return isinstance(context, FlowRunContext) or isinstance(context, TaskRunContext)

    def debug(self, *args: Any, **kwargs: Any) -> None:
        self._std_logger.debug(*args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().debug(*args, **kwargs)

    def info(self, *args: Any, **kwargs: Any) -> None:
        self._std_logger.info(*args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().info(*args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self._std_logger.warning(*args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().warning(*args, **kwargs)

    def error(self, *args: Any, **kwargs: Any) -> None:
        self._std_logger.error(*args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().error(*args, **kwargs)

    def critical(self, *args: Any, **kwargs: Any) -> None:
        self._std_logger.critical(*args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().critical(*args, **kwargs)

    def exception(self, *args: Any, **kwargs: Any) -> None:
        self._std_logger.exception(*args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().exception(*args, **kwargs)

    def log(self, *args: Any, **kwargs: Any) -> None:
        self._std_logger.log(*args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().log(*args, **kwargs)
