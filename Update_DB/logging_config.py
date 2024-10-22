from loguru import logger
import time
import requests
from db_config import *  # DB_PARAMS, SSH_TUNNEL_PARAMS

# Custom sink for Telegram logging
def telegram_sink(message):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    params = {
        'chat_id': TG_CHAT_ID,
        'text': message
    }
    requests.get(url, params=params)

class ExecutionTimeFilter:
    def __init__(self):
        self.start_time = time.time()

    def __call__(self, record):
        if 'execution_time' not in record["extra"]:
            record["extra"]["execution_time"] = f"{time.time() - self.start_time:.2f}s"
        return True

# Function to configure logger
def logger_config():
    # Remove default handler
    logger.remove()
    
    # Create execution time filter
    execution_filter = ExecutionTimeFilter()
    
    # Add console handler with colors and execution time
    logger.add(
        sink=lambda msg: print(msg, end=''),
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<yellow>{extra[execution_time]: ^8}</yellow> | "
            "<level>{level: ^8}</level> | "
            "<cyan>{name: ^19}</cyan> | "
            "<cyan>{function: ^28}</cyan> | "
            "<level>{message}</level>"
        ),
        level="INFO",
        colorize=True,
        filter=execution_filter
    )
    
    # Add Telegram handler with execution time
    logger.add(
        telegram_sink,
        format="[{time:YYYY-MM-DD HH:mm:ss}] [{extra[execution_time]}] [{level}] {name} - {message}",
        level="INFO",
        filter=execution_filter
    )
    
    return logger

# Decorator for logging function execution
def log_function_execution(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        function_name = f"{func.__name__: <28}"
        logger.info(f"{function_name} | {'Start function': <17} | {' ' * 8}")
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Updating the value of execution_time
        execution_time_str = f"{execution_time:.2f}s".ljust(8)
        logger.info(
            f"{function_name} | {'Function executed': <17} | {execution_time_str: ^8}", 
            extra={"execution_time": f"{execution_time:.2f}s"}
        )
        
        return result
    return wrapper

# Configure logger
logger = logger_config()
