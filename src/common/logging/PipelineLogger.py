import logging
from logging import Logger

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

    def debug(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.debug(msg, *args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.info(msg, *args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.warning(msg, *args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.error(msg, *args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().error(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.critical(msg, *args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().critical(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.exception(msg, *args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().exception(msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.log(level, msg, *args, **kwargs)
        if self.__running_on_prefect():
            prefect.logging.get_run_logger().log(level, msg, *args, **kwargs)
