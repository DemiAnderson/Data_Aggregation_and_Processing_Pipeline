import logging


# Function to logging (decorator)
def log_function_execution(func):
    def wrapper(*args, **kwargs):
        logger.info(f"==> '{func.__name__}' - Start function")
        result = func(*args, **kwargs)
        logger.info(f"==> '{func.__name__}' - Function executed")
        return result
    return wrapper

def logger_config():
    logging.basicConfig(level=logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    for handler in logging.root.handlers:
        handler.setFormatter(formatter)

    return logger

logger = logger_config()