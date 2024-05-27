import logging
from logging import Formatter

def logger_config():
    logging.basicConfig(level=logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    for handler in logging.root.handlers:
        handler.setFormatter(formatter)

    return logger

logger = logger_config()