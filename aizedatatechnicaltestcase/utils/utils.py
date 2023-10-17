import logging
from datetime import datetime

from aizedatatechnicaltestcase.common.constants import (
    LOG_APPEND_MODE,
    LOG_DATE_FORMAT,
    LOG_FILE_TEMPLATE,
    LOG_FILE_TS_FORMAT,
    LOG_FORMAT,
    PY4J_LOGGER_NAME,
)


def get_logger(file_name: str, stream_output: bool = False) -> logging.Logger:
    """
    Function to create a logger based on config provided and returns its object.
    :param file_name: Name of the file to create the logger for.
    :param stream_output: Flag to indicate if the logger should stream output to console.
    :return: RootLogger object.
    """
    log_file = LOG_FILE_TEMPLATE.format(file_name, datetime.now().strftime(LOG_FILE_TS_FORMAT))
    logging.getLogger(PY4J_LOGGER_NAME).setLevel(logging.INFO)
    logging.basicConfig(filename=log_file, format=LOG_FORMAT, filemode=LOG_APPEND_MODE, datefmt=LOG_DATE_FORMAT)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    if stream_output:
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        console.setFormatter(logging.Formatter(LOG_FORMAT))
        logging.getLogger().addHandler(console)
    return logger
