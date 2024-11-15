from datetime import datetime, timedelta

from raw_data_fetch import *
from logging_config import *
from params import *


@log_function_execution
def fetch_external_data(
    # login: str,
    # password: str,
    download_path: str,
) -> None:
    """Запускает основной процесс."""
    # logger.info("starting Function fetch external data")
    base_date = datetime.now() - timedelta(days=1)
    
    driver = create_driver(download_path)
    
    try:
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, MAX_WAIT_TIME)
        
        # Authorization
        auth_actions = get_authorization_actions(LOGIN, PASSWORD)
        execute_actions(wait, auth_actions)
        
        # Date Processing
        dates = get_dates_to_process(base_date)
        for index, date in enumerate(dates):
            logger.info(f"processing date: {date.day:02d}.{date.month:02d}.{date.year}")
            process_date(driver, wait, download_path, date, index)
    
    finally:
        driver.quit()
        # logger.info("function fetch sexternal data executed")

if __name__ == "__main__":
    fetch_external_data(DATA["sales"]["FOLDER_PATH_IN"])
