"""
Модуль автообновления клиента через GitHub Releases.

На Windows исполняемый файл и загруженные библиотеки заблокированы, пока
процесс работает, поэтому обновление применяется в два шага:
  1. архив распаковывается во временную staging-папку;
  2. запускается отдельный bat-скрипт, который дожидается завершения процесса
     клиента, копирует файлы поверх старых и перезапускает клиент.

На POSIX-системах используется классическая схема распаковки + os.execv.
"""
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import zipfile

import requests
from version import VERSION

GITHUB_REPO = "Ottiks17/XMPP2"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"


def get_latest_release():
    """Получить информацию о последнем релизе с GitHub."""
    try:
        response = requests.get(GITHUB_API_URL, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None


def compare_versions(current, latest):
    """Сравнить версии. Возвращает True если latest новее."""
    try:
        current_parts = [int(x) for x in current.strip("v").split(".")]
        latest_parts = [int(x) for x in latest.strip("v").split(".")]
        return latest_parts > current_parts
    except Exception:
        return False


def download_update(url, dest_path, progress_callback=None):
    """Скачать файл обновления."""
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))
        downloaded = 0
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                if progress_callback and total:
                    progress_callback(int(downloaded / total * 100))
        return True
    except Exception:
        return False


def _application_dir() -> str:
    """Директория установки приложения (или откуда запущен скрипт)."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def install_update(zip_path):
    """Распаковать обновление и перезапустить клиент.

    На Windows применяется двухшаговая схема со staging-папкой и bat-скриптом,
    потому что запущенный исполняемый файл нельзя перезаписать напрямую.
    """
    try:
        app_dir = _application_dir()
        if sys.platform == "win32":
            _install_windows(zip_path, app_dir)
        else:
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(app_dir)
            os.remove(zip_path)
            os.execv(sys.executable, [sys.executable] + sys.argv)
        return True
    except Exception:
        return False


def _install_windows(zip_path: str, app_dir: str) -> None:
    """Windows: распаковка в staging + запуск bat-скрипта замены и перезапуска."""
    staging = tempfile.mkdtemp(prefix="xmpp_update_")
    script_path = os.path.join(
        tempfile.gettempdir(), f"xmpp_update_{os.getpid()}.bat"
    )
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(staging)
        os.remove(zip_path)

        _write_update_script(
            script_path,
            staging=staging,
            app_dir=app_dir,
            executable=sys.executable,
            pid=os.getpid(),
        )

        # Скрипт запускается отдельно от процесса клиента (detached), поэтому
        # он не будет убит вместе с клиентом и сможет его перезапустить.
        subprocess.Popen(
            ["cmd.exe", "/c", script_path],
            creationflags=_detached_flags(),
            close_fds=True,
        )
    except Exception:
        # staging и скрипт не нужны, если запустить обновление не удалось.
        shutil.rmtree(staging, ignore_errors=True)
        try:
            os.remove(script_path)
        except OSError:
            pass
        raise


def _detached_flags() -> int:
    """Флаги CreateProcess, чтобы bat-скрипт пережил завершение клиента."""
    flags = 0
    for name in ("CREATE_NEW_PROCESS_GROUP", "DETACHED_PROCESS", "CREATE_NO_WINDOW"):
        flags |= getattr(subprocess, name, 0)
    return flags


def _write_update_script(script_path, *, staging, app_dir, executable, pid):
    """Пишет bat-скрипт, который после завершения клиента применяет обновление.

    Пути встраиваются в текст скрипта напрямую, чтобы не зависеть от правил
    квотирования аргументов cmd.exe. Внутри двойных кавычек спецсимволы
    &|<>^ литеральны, поэтому экранируется только '%' (в bat это метасимвол).
    """
    def _q(value):
        return '"' + str(value).replace("%", "%%") + '"'

    staging_glob = _q(os.path.join(staging, "*"))
    app_dir_dst = _q(app_dir + os.sep)
    exe = _q(executable)
    pid_s = str(pid)

    lines = [
        "@echo off",
        "setlocal",
        "rem Ждём завершения процесса клиента (PID %s)." % pid_s,
        ":waitloop",
        'tasklist /FI "PID eq %s" 2>NUL | find "%s" >NUL' % (pid_s, pid_s),
        "if not errorlevel 1 (",
        "  ping -n 2 127.0.0.1 >NUL",
        "  goto waitloop",
        ")",
        "",
        "rem Копируем файлы обновления поверх установленных.",
        "xcopy /E /Y /I %s %s >NUL" % (staging_glob, app_dir_dst),
        "",
        "rem Удаляем staging-папку.",
        "rmdir /S /Q %s" % _q(staging),
        "",
        "rem Перезапускаем клиент.",
        "start \"\" %s" % exe,
        "",
        "rem Удаляем сам скрипт.",
        'del "%~f0" >NUL 2>&1',
        "endlocal",
    ]
    with open(script_path, "w", encoding="utf-8", newline="") as f:
        f.write("\r\n".join(lines) + "\r\n")


def check_for_updates(callback):
    """
    Проверить обновления в фоновом потоке.
    callback(has_update, latest_version, download_url) вызывается когда проверка завершена.
    """
    def _check():
        release = get_latest_release()
        if not release:
            callback(False, None, None)
            return
        latest_version = release.get("tag_name", "").strip("v")
        if not compare_versions(VERSION, latest_version):
            callback(False, None, None)
            return
        # Найти zip файл в assets
        download_url = None
        for asset in release.get("assets", []):
            if asset["name"].endswith(".zip"):
                download_url = asset["browser_download_url"]
                break
        # Не предлагать обновление, если нечего скачивать (нет zip-ассета).
        if download_url is None:
            callback(False, None, None)
            return
        callback(True, latest_version, download_url)

    threading.Thread(target=_check, daemon=True).start()
