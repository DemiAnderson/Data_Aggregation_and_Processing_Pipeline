import logging
import time
import colorlog

from requests import get
from db_config import *  # DB_PARAMS, SSH_TUNNEL_PARAMS

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
        start_time = time.time()
        logger.info(f"==> '{func.__name__}' - Start function")
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"==> '{func.__name__}' - Function executed. Execution time: {execution_time:.2f} sec.")
        
        return result
    return wrapper

def logger_config():
    # Create logging
    logging.basicConfig(level=logging.INFO)
    # formatter = logging.Formatter('[%(asctime)s] [%(levelname)-7s] %(module)-20s %(message)s')
    # Create a colorized formatter
    formatter = colorlog.ColoredFormatter(
        '%(log_color)s[%(asctime)s] [%(levelname)-7s] %(module)-20s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red,bg_white',
        },
        secondary_log_colors={},
        style='%'
    )
    
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