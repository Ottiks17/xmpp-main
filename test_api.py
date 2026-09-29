import json
import os

import requests

CONFIG_PATH = "config/config.json"
DEFAULT_URL = "http://127.0.0.1:8080/send_message"

config = {}
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
        config = json.load(f)

rest_cfg = config.get("rest_api", {})
host = rest_cfg.get("host", "127.0.0.1")
port = rest_cfg.get("port", 8080)
endpoint = rest_cfg.get("endpoint", "/send_message")
api_key = rest_cfg.get("api_key", "")

if host in ("0.0.0.0", "::"):
    host = "127.0.0.1"

url = f"http://{host}:{port}{endpoint}"

test_message = {
    "to": "testuser",
    "message": "Привет! Тест API. Русские буквы, English, 123.",
}

headers = {"Content-Type": "application/json"}
if api_key:
    headers["X-API-Key"] = api_key

try:
    response = requests.post(url, json=test_message, headers=headers, timeout=10)
    print(f"URL: {url}")
    print(f"Status code: {response.status_code}")
    print(f"Response: {response.json()}")
except Exception as e:
    print(f"Error: {e}")

