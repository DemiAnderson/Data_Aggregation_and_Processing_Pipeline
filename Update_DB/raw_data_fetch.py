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

# Import configuration from external files
from db_config import *
from exception_config import * # exception
from logging_config import * # logger
from params import *


# Function to create an action
def create_action(action_type: str, locator: Tuple[str, str], text: Optional[str] = None) -> Dict:
    """
    Creates a dictionary representing an action to be executed by the Selenium WebDriver.

    Args:
        action_type (str): The type of action (e.g., 'click', 'input').
        locator (Tuple[str, str]): The locator to identify the element (strategy, value).
        text (Optional[str]): Optional text to input into the element.

    Returns:
        Dict: A dictionary representing the action.
    """
    return {
        "action_type": action_type,
        "locator": locator,
        "text": text
    }

# Function to configure Chrome options
def get_chrome_options(download_path: str) -> Options:
    """
    Configures and returns Chrome options for the WebDriver.

    Args:
        download_path (str): The default download directory for files.

    Returns:
        Options: A configured Chrome Options object.
    """
    chrome_options = Options()
    prefs = {**CHROME_PREFS, "download.default_directory": download_path}
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Add arguments to suppress various errors
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-features=VizDisplayCompositor')
    chrome_options.add_argument('--disable-features=IsolateOrigins,site-per-process')
    
    # Suppress logging
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_argument('--silent')
    
    # Uncomment the next line to enable headless mode
    # chrome_options.add_argument('--headless')
    
    return chrome_options

# Function to create a Chrome driver
def create_driver(download_path: str) -> webdriver.Chrome:
    """
    Creates and returns a configured instance of the Chrome WebDriver.

    Args:
        download_path (str): The default download directory for files.

    Returns:
        webdriver.Chrome: A configured Chrome WebDriver instance.
    """
    options = get_chrome_options(download_path)
    
    # Suppress DevTools logs
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument('--disable-logging')
    options.add_argument('--log-level=3')
    
    # Suppress console output
    os.environ['WDM_LOG_LEVEL'] = '0'
    
    return webdriver.Chrome(options=options)

# Function to get dates for processing
def get_dates_to_process(base_date: datetime) -> List[datetime]:
    """
    Returns a list of dates to process based on the given date.

    Args:
        base_date (datetime): The base date to calculate from.

    Returns:
        List[datetime]: A list of dates for processing.
    """
    if base_date.weekday() == 5:  # суббота
        return [base_date - timedelta(days=i) for i in range(PREVIOUS_DAYS)]
    return [base_date]

# Function to calculate calendar position
def calculate_calendar_position(date: datetime) -> Tuple[int, int]:
    """
    Calculates the position of a date in a calendar grid.

    Args:
        date (datetime): The date for which to calculate the position.

    Returns:
        Tuple[int, int]: The week and day of the week position in the calendar grid.
    """
    first_day = date.replace(day=1)
    first_weekday = first_day.weekday()
    
    # Adjustment for Sunday
    if first_weekday == 6:  
        first_weekday = -1
    
    week = (date.day + first_weekday) // 7 + 1
    weekday = (date.weekday() + 1) % 7 + 1
    return week, weekday

# Function to generate a date selector
def get_date_selector(date: datetime, index: int = 0) -> str:
    """
    Generates a CSS selector for selecting a specific date in a calendar.

    Args:
        date (datetime): The date to select.
        index (int): Index of the calendar (0 for the first calendar).

    Returns:
        str: The generated CSS selector.
    """
    week, weekday = calculate_calendar_position(date)
    base_selector = f"tr:nth-child({week}) > td:nth-child({weekday})"
    
    if index == 0:
        return f"body > div.datepicker.dropdown-menu > div.datepicker-days > table > tbody > {base_selector}"
    
    calendar_index = 70 + (index - 1) * 2
    return f"body > div:nth-child({calendar_index}) > div.datepicker-days > table > tbody > {base_selector}"

# Function to create authorization actions
def get_authorization_actions(login: str, password: str) -> List[Dict]:
    """
    Creates a list of actions for user authorization.

    Args:
        login (str): The user's login.
        password (str): The user's password.

    Returns:
        List[Dict]: A list of actions to perform authorization.
    """
    return [
        create_action('click', (By.ID, "email")),
        create_action('input', (By.ID, "email"), login),
        create_action('click', (By.ID, "continue")),
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

# Function to create actions for processing a specific date
def execute_action(wait: WebDriverWait, action: Dict) -> None:
    """
    Creates a list of actions to process a specific date.

    Args:
        date_selector (str): The CSS selector for the date to process.

    Returns:
        List[Dict]: A list of actions for processing the date.
    """
    element = wait.until(EC.element_to_be_clickable(action["locator"]))
    
    if action["action_type"] == 'click':
        element.click()
    elif action["action_type"] == 'input' and action["text"]:
        element.clear()
        element.send_keys(action["text"])

# Function to retry an action with multiple attempts
def retry_action(action_func: Callable, max_attempts: int = DEFAULT_RETRY_ATTEMPTS) -> None:
    """
    Retries the execution of an action multiple times in case of specific exceptions.

    Args:
        action_func (Callable): The action function to execute.
        max_attempts (int): The maximum number of retry attempts.

    Raises:
        Exception: Re-raises the last encountered exception if all retries fail.
    """
    for attempt in range(max_attempts):
        try:
            action_func()
            return
        except (TimeoutException, StaleElementReferenceException):
            if attempt == max_attempts - 1:
                raise

# Function to execute a list of actions sequentially
def execute_actions(wait: WebDriverWait, actions: List[Dict]) -> None:
    """
    Executes a list of Selenium actions sequentially, retrying each action on failure.

    Args:
        wait (WebDriverWait): The WebDriverWait instance to manage timeouts.
        actions (List[Dict]): A list of actions to execute in sequence.

    Raises:
        Exception: Propagates exceptions from individual actions if retries fail.
    """
    for action in actions:
        retry_action(lambda: execute_action(wait, action))

# Function to check if the expected file is downloaded
def check_file_downloaded(download_path: str, expected_path: str) -> bool:
    """
    Checks if the expected file has been downloaded, renames it, and verifies success.

    Args:
        download_path (str): The directory where downloads are stored.
        expected_path (str): The target path for renaming the downloaded file.

    Returns:
        bool: True if the file was successfully renamed, False otherwise.

    Logs:
        Logs an error if renaming fails.
    """
    for file in os.listdir(download_path):
        if file.startswith("TurnoverList.xlsx"):
            old_file = os.path.join(download_path, file)
            try:
                if os.path.exists(expected_path):
                    os.remove(expected_path)  # Remove existing file
                os.rename(old_file, expected_path)
                return True
            except OSError as e:
                logger.error(f"Error renaming file: {e}")
                return False
    return False

# Function to wait for a file to finish downloading
def wait_for_file(download_path: str, expected_path: str) -> None:
    """
    Waits for a specific file to finish downloading.

    Args:
        download_path (str): The directory where downloads are stored.
        expected_path (str): The expected path for the downloaded file.

    Raises:
        TimeoutException: If the file is not downloaded within the maximum wait time.
    """
    checker = partial(check_file_downloaded, download_path, expected_path)
    WebDriverWait(None, MAX_WAIT_TIME).until(lambda _: checker())

# Function to process a single date
def process_date(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    download_path: str,
    date: datetime,
    index: int
) -> None:
    """
    Processes a single date by performing necessary actions and downloading a file.

    Args:
        driver (webdriver.Chrome): The Selenium WebDriver instance.
        wait (WebDriverWait): The WebDriverWait instance to handle timeouts.
        download_path (str): The directory where downloads are stored.
        date (datetime): The date to process.
        index (int): The index of the calendar to interact with.

    Raises:
        Exception: If actions fail or the file is not downloaded successfully.
    """
    date_selector = get_date_selector(date, index)
    formatted_date = date.strftime("%d.%m.%y")
    file_path = os.path.join(download_path, f"TurnoverList ({formatted_date}).xlsx")
    
    actions = get_processing_actions(date_selector)
    execute_actions(wait, actions)
    wait_for_file(download_path, file_path)


