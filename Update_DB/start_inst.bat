@echo off
REM Активируем виртуальное окружение
call "C:\Users\dmandree\OneDrive - Guess Inc\D Project\Worked\test_script_development\Update_DB\venv\Scripts\activate.bat"
pip install psycopg2-binary openpyxl

REM Запускаем Python скрипт
python "C:\Users\dmandree\OneDrive - Guess Inc\D Project\Worked\test_script_development\Update_DB\main.py"
pause
