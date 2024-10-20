import shutil

import functools
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sshtunnel import SSHTunnelForwarder, BaseSSHTunnelForwarderError

from logging_config import * # logger


# Decorators
# Function to fix exceptions (decorator)
def exception(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            logger.error(f"File not found error in {func.__name__}: {e}")
        except PermissionError as e:
            logger.warning(f"Permission error in {func.__name__}: {e}")
        except (IOError, shutil.Error) as e:
            logger.error(f"Error while moving file {func.__name__}: {e}")
        except ValueError as e:
            logger.error(f"Value error in {func.__name__}: {e}")
        except pd.errors.ParserError as e:
            logger.error(f"Parser error in {func.__name__}: {e}")
        except OSError as e:
            logger.error(f"OS error in {func.__name__}: {e}")
        except BaseSSHTunnelForwarderError as e:
            logger.error(f"SSH tunnel error in {func.__name__}: {e}")
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error in {func.__name__}: {e}")
        except AttributeError as e:
            logger.error(f"Attribute error in {func.__name__}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}")
        logger.error(f"'{func.__name__}' - Function failed")            
        return None
    return wrapper

