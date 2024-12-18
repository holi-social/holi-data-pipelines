import logging
from typing import Union

import prefect.logging


class PipelineLogger:
    def __init__(self, logger_name: str, prefect_level_threshold: Union[str, int] = "INFO"):
        """
        @param logger_name: name of the logger
        @param prefect_level_threshold: threshold of the level from which logs also go to prefect
        """
        self._std_logger = logging.getLogger(logger_name)
        self._prefect_logger = prefect.logging.get_logger(logger_name)
        self._prefect_logger.setLevel(prefect_level_threshold)

    def debug(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.debug(msg, *args, **kwargs)
        self._prefect_logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.info(msg, *args, **kwargs)
        self._prefect_logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.warning(msg, *args, **kwargs)
        self._prefect_logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.error(msg, *args, **kwargs)
        self._prefect_logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.critical(msg, *args, **kwargs)
        self._prefect_logger.critical(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.exception(msg, *args, **kwargs)
        self._prefect_logger.exception(msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args, **kwargs):  # type: ignore
        self._std_logger.log(level, msg, *args, **kwargs)
        self._prefect_logger.log(level, msg, *args, **kwargs)