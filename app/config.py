import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from app.constants import CONFIG_PATH

load_dotenv()


def deep_merge(base: dict, override: dict) -> dict:
    result = deepcopy(base)
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def default_config() -> dict[str, Any]:
    return {
        "xmpp": {
            "server": "192.168.2.201",
            "port": 5222,
            "username": "",
            "password": "",
            "use_tls": True,
            "verify_tls": False,
            "use_slixmpp": False,
        },
        "rest_api": {
            "host": "127.0.0.1",
            "port": 8080,
            "endpoint": "/send_message",
            "api_key": "",
            "allow_get": False,
        },
        "kafka": {
            "enabled": False,
            "bootstrap_servers": "localhost:9092",
            "topics": {
                "messages": "xmpp-messages",
                "api_requests": "xmpp-api-requests",
                "events": "xmpp-events",
            },
            "consumer_group": "xmpp-client",
            "use_for_api_queue": True,
            "consume_api_requests": True,
            "auto_offset_reset": "latest",
            "publish_all_messages": True,
            "publish_api_requests": True,
        },
        "logging": {
            "retention_days": 14,
        },
    }


def apply_env_overrides(config: dict[str, Any]) -> dict[str, Any]:
    xmpp = config.setdefault("xmpp", {})
    if os.getenv("XMPP_SERVER"):
        xmpp["server"] = os.getenv("XMPP_SERVER")
    if os.getenv("XMPP_PORT"):
        xmpp["port"] = int(os.getenv("XMPP_PORT"))
    if os.getenv("XMPP_USERNAME"):
        xmpp["username"] = os.getenv("XMPP_USERNAME")
    if os.getenv("XMPP_PASSWORD"):
        xmpp["password"] = os.getenv("XMPP_PASSWORD")

    rest = config.setdefault("rest_api", {})
    if os.getenv("REST_API_KEY"):
        rest["api_key"] = os.getenv("REST_API_KEY")
    if os.getenv("REST_HOST"):
        rest["host"] = os.getenv("REST_HOST")
    if os.getenv("REST_PORT"):
        rest["port"] = int(os.getenv("REST_PORT"))

    kafka = config.setdefault("kafka", {})
    if os.getenv("KAFKA_ENABLED"):
        kafka["enabled"] = os.getenv("KAFKA_ENABLED").lower() in ("true", "1", "yes")
    if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
        kafka["bootstrap_servers"] = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if os.getenv("KAFKA_CONSUMER_GROUP"):
        kafka["consumer_group"] = os.getenv("KAFKA_CONSUMER_GROUP")
    if os.getenv("KAFKA_USE_FOR_API_QUEUE"):
        kafka["use_for_api_queue"] = os.getenv("KAFKA_USE_FOR_API_QUEUE").lower() in (
            "true",
            "1",
            "yes",
        )
    if os.getenv("KAFKA_PUBLISH_ALL_MESSAGES"):
        kafka["publish_all_messages"] = os.getenv(
            "KAFKA_PUBLISH_ALL_MESSAGES"
        ).lower() in ("true", "1", "yes")
    if os.getenv("KAFKA_PUBLISH_API_REQUESTS"):
        kafka["publish_api_requests"] = os.getenv(
            "KAFKA_PUBLISH_API_REQUESTS"
        ).lower() in ("true", "1", "yes")
    if os.getenv("KAFKA_CONSUME_API_REQUESTS"):
        kafka["consume_api_requests"] = os.getenv(
            "KAFKA_CONSUME_API_REQUESTS"
        ).lower() in ("true", "1", "yes")
    if os.getenv("KAFKA_AUTO_OFFSET_RESET"):
        kafka["auto_offset_reset"] = os.getenv("KAFKA_AUTO_OFFSET_RESET")

    return config


def load_config(path: str = CONFIG_PATH) -> dict[str, Any]:
    config = default_config()
    config_path = Path(path)
    if config_path.exists():
        try:
            with config_path.open("r", encoding="utf-8-sig") as handle:
                loaded = json.load(handle)
            config = deep_merge(config, loaded)
        except (json.JSONDecodeError, OSError):
            pass
    config = apply_env_overrides(config)
    return config


def save_config(config: dict[str, Any], path: str = CONFIG_PATH) -> None:
    config_path = Path(path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8") as handle:
        json.dump(config, handle, ensure_ascii=False, indent=2)

