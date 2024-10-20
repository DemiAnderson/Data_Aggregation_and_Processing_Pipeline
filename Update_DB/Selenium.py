from typing import List, Tuple, Dict, Optional, Callable
from datetime import datetime, timedelta
import os
from functools import partial

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

# Импорт конфигурации из внешних файлов
from db_config import *
from exception_config import * # exception
from logging_config import * # logger
from params import *


# Базовая конфигурация браузера
CHROME_PREFS = {
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}

def create_action(action_type: str, locator: Tuple[str, str], text: Optional[str] = None) -> Dict:
    """Создает словарь действия."""
    return {
        "action_type": action_type,
        "locator": locator,
        "text": text
    }

def get_chrome_options(download_path: str) -> Options:
    """Создает настройки для Chrome."""
    chrome_options = Options()
    prefs = {**CHROME_PREFS, "download.default_directory": download_path}
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Добавляем аргументы для отключения ошибок
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-features=VizDisplayCompositor')
    chrome_options.add_argument('--disable-features=IsolateOrigins,site-per-process')
    
    # Отключаем логирование
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_argument('--silent')
    
    # Добавляем опцию для запуска в безголовом режиме
    # chrome_options.add_argument('--headless')
    
    return chrome_options

def create_driver(download_path: str) -> webdriver.Chrome:
    """Создает настроенный экземпляр драйвера."""
    options = get_chrome_options(download_path)
    
    # Добавляем опции для отключения логов DevTools
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument('--disable-logging')
    options.add_argument('--log-level=3')
    
    # Отключаем вывод сообщений в консоль
    os.environ['WDM_LOG_LEVEL'] = '0'
    
    return webdriver.Chrome(options=options)

def get_dates_to_process(base_date: datetime) -> List[datetime]:
    """Возвращает список дат для обработки."""
    if base_date.weekday() == 5:  # суббота
        return [base_date - timedelta(days=i) for i in range(PREVIOUS_DAYS)]
    return [base_date]

def calculate_calendar_position(date: datetime) -> Tuple[int, int]:
    """Вычисляет позицию даты в календаре."""
    first_day = date.replace(day=1)
    first_weekday = first_day.weekday()
    
    # Корректировка для воскресенья
    if first_weekday == 6:  # Если первый день - воскресенье
        first_weekday = -1
    
    week = (date.day + first_weekday) // 7 + 1
    weekday = (date.weekday() + 1) % 7 + 1
    return week, weekday

def get_date_selector(date: datetime, index: int = 0) -> str:
    """Генерирует CSS-селектор для выбора даты."""
    week, weekday = calculate_calendar_position(date)
    base_selector = f"tr:nth-child({week}) > td:nth-child({weekday})"
    
    if index == 0:
        return f"body > div.datepicker.dropdown-menu > div.datepicker-days > table > tbody > {base_selector}"
    
    calendar_index = 70 + (index - 1) * 2
    return f"body > div:nth-child({calendar_index}) > div.datepicker-days > table > tbody > {base_selector}"

def get_authorization_actions(login: str, password: str) -> List[Dict]:
    """Создает список действий для авторизации."""
    return [
        create_action('click', (By.ID, "GuessAzureAD")),
        create_action('input', (By.ID, "i0116"), login),
        create_action('click', (By.ID, "idSIButton9")),
        create_action('input', (By.ID, "i0118"), password),
        create_action('click', (By.ID, "idSIButton9"))
    ]

def get_processing_actions(date_selector: str) -> List[Dict]:
    """Создает список действий для обработки даты."""
    return [
        create_action('click', (By.CSS_SELECTOR, "button.btn-block")),
        create_action('click', (By.CSS_SELECTOR, date_selector)),
        create_action('click', (By.CSS_SELECTOR, "button.red")),
        create_action('click', (By.CSS_SELECTOR, "div.col-md-6:nth-child(2) > div:nth-child(1) > ul:nth-child(1) > li:nth-child(2) > ul:nth-child(1) > li:nth-child(1) > button:nth-child(1) > i:nth-child(1)"))
    ]

def execute_action(wait: WebDriverWait, action: Dict) -> None:
    """Выполняет отдельное действие."""
    element = wait.until(EC.element_to_be_clickable(action["locator"]))
    
    if action["action_type"] == 'click':
        element.click()
    elif action["action_type"] == 'input' and action["text"]:
        element.clear()
        element.send_keys(action["text"])

def retry_action(action_func: Callable, max_attempts: int = DEFAULT_RETRY_ATTEMPTS) -> None:
    """Выполняет действие с повторными попытками."""
    for attempt in range(max_attempts):
        try:
            action_func()
            return
        except (TimeoutException, StaleElementReferenceException):
            if attempt == max_attempts - 1:
                raise

def execute_actions(wait: WebDriverWait, actions: List[Dict]) -> None:
    """Выполняет список действий последовательно."""
    for action in actions:
        retry_action(lambda: execute_action(wait, action))

def check_file_downloaded(download_path: str, expected_path: str) -> bool:
    """Проверяет загрузку файла и переименовывает его."""
    for file in os.listdir(download_path):
        if file.startswith("TurnoverList.xlsx"):
            old_file = os.path.join(download_path, file)
            try:
                if os.path.exists(expected_path):
                    os.remove(expected_path)  # Удаляем существующий файл
                os.rename(old_file, expected_path)
                return True
            except OSError as e:
                logger.error(f"Ошибка при переименовании файла: {e}")
                return False
    return False

def wait_for_file(download_path: str, expected_path: str) -> None:
    """Ожидает загрузку файла."""
    checker = partial(check_file_downloaded, download_path, expected_path)
    WebDriverWait(None, MAX_WAIT_TIME).until(lambda _: checker())

def process_date(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    download_path: str,
    date: datetime,
    index: int
) -> None:
    """Обрабатывает одну дату."""
    date_selector = get_date_selector(date, index)
    formatted_date = date.strftime("%d.%m.%y")
    file_path = os.path.join(download_path, f"TurnoverList ({formatted_date}).xlsx")
    
    actions = get_processing_actions(date_selector)
    execute_actions(wait, actions)
    wait_for_file(download_path, file_path)

def run_selenium_process(
    login: str,
    password: str,
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
        auth_actions = get_authorization_actions(login, password)
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
    run_selenium_process(LOGIN, PASSWORD, DOWNLOAD_PATH)
