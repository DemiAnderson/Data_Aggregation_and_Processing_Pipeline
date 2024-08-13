import logging
from requests import get

from db_config import * # DB_PARAMS, SSH_TUNNEL_PARAMS


class TelegramHandler(logging.Handler):
    def __init__(self):
        logging.Handler.__init__(self)

    def emit(self, record):
        log_entry = self.format(record)
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        params = {
            'chat_id': TG_CHAT_ID,
            'text': log_entry
        }
        url_result = get(url, params=params)

# Function to logging (decorator)
def log_function_execution(func):
    def wrapper(*args, **kwargs):
        logger.info(f"==>      '{func.__name__}' - Start function")
        result = func(*args, **kwargs)
        logger.info(f"==>      '{func.__name__}' - Function executed")
        return result
    return wrapper

def logger_config():
    # Create logging
    logging.basicConfig(level=logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)-7s] %(module)-20s %(message)s')
    
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Adding the default handlers
    for handler in logging.root.handlers:
        handler.setFormatter(formatter)
        
    # Adding the Telegram handler
    telegram_handler = TelegramHandler()
    telegram_handler.setLevel(logging.INFO)
    telegram_handler.setFormatter(formatter)
    logger.addHandler(telegram_handler)

    return logger

logger = logger_config()