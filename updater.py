"""
Модуль автообновления клиента через GitHub Releases.
"""
import os
import sys
import zipfile
import threading
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
        total = int(response.headers.get("content-length", 0))
        downloaded = 0
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if progress_callback and total:
                    progress_callback(int(downloaded / total * 100))
        return True
    except Exception:
        return False


def install_update(zip_path):
    """Распаковать обновление и перезапустить клиент."""
    try:
        app_dir = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, 'frozen', False) else __file__))
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(app_dir)
        os.remove(zip_path)
        # Перезапуск
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        return False
    return True


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
        if compare_versions(VERSION, latest_version):
            # Найти zip файл в assets
            download_url = None
            for asset in release.get("assets", []):
                if asset["name"].endswith(".zip"):
                    download_url = asset["browser_download_url"]
                    break
            callback(True, latest_version, download_url)
        else:
            callback(False, None, None)

    threading.Thread(target=_check, daemon=True).start()

