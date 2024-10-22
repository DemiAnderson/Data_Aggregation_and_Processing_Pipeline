from datetime import datetime, timedelta

from raw_data_fetch import *
from logging_config import *
from params import *


def fetch_external_data(
    # login: str,
    # password: str,
    download_path: str,
) -> None:
    """Запускает основной процесс."""
    logger.info("Starting Function Selenium")
    base_date = datetime.now() - timedelta(days=1)
    
    driver = create_driver(download_path)
    
    try:
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, MAX_WAIT_TIME)
        
        # Авторизация
        auth_actions = get_authorization_actions(LOGIN, PASSWORD)
        execute_actions(wait, auth_actions)
        
        # Обработка дат
        dates = get_dates_to_process(base_date)
        for index, date in enumerate(dates):
            logger.info(f"Processing date: {date.day:02d}.{date.month:02d}.{date.year}")
            process_date(driver, wait, download_path, date, index)
    
    finally:
        driver.quit()
        logger.info("Function Selenium executed")

if __name__ == "__main__":
    fetch_external_data(DOWNLOAD_PATH)
