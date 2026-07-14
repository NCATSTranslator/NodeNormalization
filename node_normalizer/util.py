import logging
import os
from logging.config import dictConfig
from logging.handlers import RotatingFileHandler
from fastapi.logger import logger as fastapi_logger

# Some constants.
BIOLINK_NAMED_THING = "biolink:NamedThing"

def get_numerical_curie_suffix(curie):
    """
    If a CURIE has a numerical suffix, return it as an integer. Otherwise return None.
    :param curie: A CURIE.
    :return: An integer if the CURIE suffix is castable to int, otherwise None.
    """
    curie_parts = curie.split(":", 1)
    if len(curie_parts) > 0:
        # Try to cast the CURIE suffix to an integer. If we get a ValueError, don't worry about it.
        try:
            return int(curie_parts[1])
        except ValueError:
            pass
    return None

# loggers = {}
class LoggingUtil(object):
    """ Logging utility controlling format and setting initial logging level """
    @staticmethod
    def init_logging(log_file_path=None, log_file_level=None):
        # If log_file_path is set, we use that. Otherwise, we use the LOG_LEVEL environmental variable.
        if not log_file_level:
            log_file_level = os.getenv("LOG_LEVEL", "INFO")

        dictConfig({
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"default": {"format": "%(asctime)s | %(levelname)s | %(module)s:%(funcName)s | %(message)s"}},
            "handlers": {
                "console": {"level": log_file_level, "class": "logging.StreamHandler", "formatter": "default"},
            },
            "loggers": {
                "node-norm": {"handlers": ["console"], "level": log_file_level},
            },
        })
        # add gunicorn handlers and configure fastapi loggers
        logger = logging.getLogger("node-norm")
        gunicorn_error_logger = logging.getLogger("gunicorn.error")
        gunicorn_logger = logging.getLogger("gunicorn")
        uvicorn_access_logger = logging.getLogger("uvicorn.access")
        uvicorn_access_logger.handlers = gunicorn_error_logger.handlers
        fastapi_logger.handlers = gunicorn_error_logger.handlers
        fastapi_logger.setLevel(gunicorn_logger.level)
        logger.handlers += gunicorn_logger.handlers
        # if there was a file path passed in use it
        if log_file_path is not None:
            # create a rotating file handler, 100mb max per file with a max number of 10 files
            file_handler = RotatingFileHandler(filename=log_file_path + 'nn.log', maxBytes=1000000, backupCount=10)

            # set the formatter
            formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(module)s:%(funcName)s | %(message)s")
            file_handler.setFormatter(formatter)

            # set the log level
            file_handler.setLevel(log_file_level)

            # add the handler to the logger
            logger.addHandler(file_handler)

        # return to the caller
        return logger


def uniquify_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]