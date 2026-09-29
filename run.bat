@echo off
setlocal
cd /d "%~dp0"

where python >nul 2>&1
if errorlevel 1 (
    echo Python не найден. Установите Python 3.10+ с python.org
    exit /b 1
)

if exist "venv\Scripts\python.exe" (
    venv\Scripts\python.exe -c "import sys" >nul 2>&1
    if errorlevel 1 (
        echo Удаление повреждённого venv...
        rmdir /s /q venv
    )
)

if not exist "venv\Scripts\python.exe" (
    echo Создание виртуального окружения...
    python -m venv venv
)

call venv\Scripts\activate.bat
python -m pip install --upgrade pip -q
pip install -r requirements.txt -q
if errorlevel 1 (
    echo Ошибка установки зависимостей
    exit /b 1
)

python main.py

