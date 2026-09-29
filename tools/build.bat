pip install pyinstaller
rem Иконки icon.ico в проекте нет — если появится, добавьте --icon=icon.ico
pyinstaller --onefile --windowed --name "XMPPClient" main.py
pause

