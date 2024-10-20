# Стандартные библиотеки
import csv
import sys
import os
import glob
from datetime import datetime, timedelta
from time import sleep

# Сторонние библиотеки
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

from db_config import * # LOGIN, PASSWORD


# Определяем вчерашнюю дату
yesterday = datetime.now() - timedelta(days=1)

PREVIOUS_DAYS = 29

def get_date_list(yesterday):
    """Возвращает список дат для обработки."""
    if yesterday.weekday() == 6:  # 6 соответствует воскресенью
        return [yesterday - timedelta(days=i) for i in range(PREVIOUS_DAYS)]
    return [yesterday]

# Получаем список дат
dates = get_date_list(yesterday)

# Определяем date_str для имени файла (первая дата в списке)
date_str = dates[0]

# Определяем путь для сохранения файла
download_path = r"C:\Users\dmandree\Downloads\TL_new" 

# Формируем имя файла
file_name = f"TurnoverList ({date_str}).xlsx"

# Настройка опций Chrome
chrome_options = Options()
prefs = {
    "download.default_directory": download_path,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "download.default_filename": file_name  # Устанавливаем имя файла по умолчанию
}
chrome_options.add_experimental_option("prefs", prefs)

url = "https://smrt.guess.eu/turnover/list#/byparams/"

# Инициализация веб-драйвера
driver = webdriver.Chrome(options=chrome_options)
driver.get(url)

# Ожидание появления кнопки и клик по ней
wait = WebDriverWait(driver, 20)

# Определение функции interact_with_element
def interact_with_element(locator, action='click', text=None, max_attempts=3):
    """Взаимодействует с элементом на странице."""
    for attempt in range(max_attempts):
        try:
            element = wait.until(EC.presence_of_element_located(locator))
            if action == 'click':
                element.click()
            elif action == 'input':
                element.clear()
                element.send_keys(text)
            return
        except (TimeoutException, StaleElementReferenceException):
            if attempt == max_attempts - 1:
                raise
            sleep(1)

def get_date_selector(date, index=0):
    """
    Возвращает CSS-селектор для выбора даты в календаре.

    Args:
        date (datetime): Дата для выбора.
        index (int): Индекс календаря на странице.

    Returns:
        str: CSS-селектор для выбора даты.
    """
    # Получаем первый день месяца
    first_day = date.replace(day=1)
    # Определяем, с какого дня недели начинается месяц (0 - понедельник, 6 - воскресенье)
    first_weekday = first_day.weekday()
    # Вычисляем номер недели и дня недели для нужной даты
    week = (date.day + first_weekday - 1) // 7 + 1
    weekday = (date.weekday() + 1) % 7 + 1  # +1, так как CSS-селекторы начинаются с 1

    if index == 0:
        # Для первого календаря используем стандартный селектор
        return f"body > div.datepicker.dropdown-menu > div.datepicker-days > table > tbody > tr:nth-child({week}) > td:nth-child({weekday})"
    else:
        # Для последующих календарей используем динамический селектор
        # 70 - это базовое значение для второго календаря
        # (index - 1) * 2 - увеличиваем на 2 для каждого следующего календаря
        selector = f"body > div:nth-child({70 + (index - 1) * 2}) > div.datepicker-days > table > tbody > tr:nth-child({week}) > td:nth-child({weekday})"
        return selector

# Объединенный словарь действий авторизаии
authorization_actions = [
    {'action': 'click', 'locator': (By.ID, "GuessAzureAD")},
    {'action': 'input', 'locator': (By.ID, "i0116"), 'text': LOGIN},
    {'action': 'click', 'locator': (By.ID, "idSIButton9")},
    {'action': 'input', 'locator': (By.ID, "i0118"), 'text': PASSWORD},
    {'action': 'click', 'locator': (By.ID, "idSIButton9")}
]


# Функция для выполнения последовательности действий
def perform_actions(actions):
    for action in actions:
        sleep(1)
        if action['action'] == 'click':
            interact_with_element(action['locator'])
        elif action['action'] == 'input':
            interact_with_element(action['locator'], action='input', text=action['text'])

# Выполнение всех действий с обработкой исключений

perform_actions(authorization_actions)

for index, date in enumerate(dates):
    date_selector = get_date_selector(date, index)

    date = date.strftime("%d.%m.%y")
    file_name_date = f"TurnoverList ({date}).xlsx"

    # Объединенный словарь действий обработки
    processing_actions = [
        {'action': 'click', 'locator': (By.CSS_SELECTOR, "button.btn-block")},
        {'action': 'click', 'locator': (By.CSS_SELECTOR, date_selector)},
        {'action': 'click', 'locator': (By.CSS_SELECTOR, "button.red")},
        {'action': 'click', 'locator': (By.CSS_SELECTOR, "div.col-md-6:nth-child(2) > div:nth-child(1) > ul:nth-child(1) > li:nth-child(2) > ul:nth-child(1) > li:nth-child(1) > button:nth-child(1) > i:nth-child(1)")}
    ]
    
    perform_actions(processing_actions)

    sleep(1)

    # Ожидание загрузки файла
    file_path = os.path.join(download_path, file_name_date)
    max_wait_time = 10

    def file_downloaded():
        for file in os.listdir(download_path):
            if file.startswith("TurnoverList.xlsx"):
                old_file = os.path.join(download_path, file)
                os.rename(old_file, file_path)
                return True
        return False


    WebDriverWait(driver, max_wait_time).until(lambda x: file_downloaded())


driver.quit()
sys.exit(1)
