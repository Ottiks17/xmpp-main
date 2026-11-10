"""
Headless-запуск: REST API + XMPP-клиент, без PyQt5.
Используется в Docker (см. Dockerfile) и на серверах без графики.
"""
import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)
sys.path.insert(0, ROOT)

from app.config import load_config
from app.storage import MessageLogger
from app.kafka_broker import KafkaBroker
from app.xmpp.service import XMPPService
from rest_server import RESTServer

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("server_main")


def main() -> int:
    config = load_config()

    retention = config.get("logging", {}).get("retention_days", 14)
    logger = MessageLogger(retention_days=retention)

    def log_callback(msg, level="INFO"):
        log.log(getattr(logging, level, logging.INFO), msg)
        if level == "MESSAGE":
            try:
                data = json.loads(msg)
                logger.log_message(
                    msg_type=data["type"],
                    sender=data["sender"],
                    recipient=data["recipient"],
                    message=data["message"],
                    message_id=data.get("message_id"),
                    priority=data.get("priority"),
                    send_time=datetime.fromisoformat(data["send_time"]) if data.get("send_time") else None,
                    delivery_time=datetime.fromisoformat(data["delivery_time"]) if data.get("delivery_time") else None,
                    read_time=datetime.fromisoformat(data["read_time"]) if data.get("read_time") else None,
                )
            except Exception as exc:
                log.warning("MESSAGE log parse failed: %s", exc)

    kafka_broker = None
    if config.get("kafka", {}).get("enabled", False):
        try:
            kafka_broker = KafkaBroker(
                config["kafka"].get("bootstrap_servers", "localhost:9092"),
                log_callback,
            )
            if not kafka_broker.connect():
                kafka_broker = None
        except Exception as exc:
            log_callback(f"Kafka init failed: {exc}", "ERROR")

    username = config.get("xmpp", {}).get("username", "").strip()
    password = config.get("xmpp", {}).get("password", "")
    xmpp_service = None
    if username and password:
        try:
            xmpp_service = XMPPService(config, log_callback, kafka_broker)
            if xmpp_service.connect(username, password):
                log_callback("XMPP connected", "INFO")

                client = xmpp_service.client

                def on_message_received(from_jid, message, msg_id=None):
                    log_callback(f"RECEIVED from {from_jid}: {message[:80]}", "INFO")

                def on_delivery_received(msg_id):
                    log_callback(f"DELIVERED: {msg_id}", "INFO")
                    logger.mark_delivered(msg_id)

                def on_read_received(msg_id):
                    log_callback(f"READ: {msg_id}", "INFO")
                    logger.mark_read(msg_id)

                def on_send_failed(msg_id):
                    log_callback(f"SEND FAILED: {msg_id}", "ERROR")
                    logger.mark_failed(msg_id)

                client.on_message_received = on_message_received
                client.on_delivery_received = on_delivery_received
                client.on_read_received = on_read_received
                client.on_send_failed = on_send_failed
            else:
                log_callback("XMPP connect failed, REST will return 500", "ERROR")
                xmpp_service = None
        except Exception as exc:
            log_callback(f"XMPP init failed: {exc}", "ERROR")
            xmpp_service = None
    else:
        log_callback("XMPP credentials not set; REST will fail to send", "WARNING")

    def message_handler(to_user, message, priority="normal", msg_id=None,
                        msg_type="text", payload=None):
        if xmpp_service is None or not xmpp_service.client or not xmpp_service.client.is_connected:
            log_callback("XMPP not connected, cannot send", "ERROR")
            return False
        msg_id = msg_id or str(uuid.uuid4())
        try:
            result = xmpp_service.send_message(
                to_user, message, msg_id=msg_id,
                priority=priority, msg_type=msg_type, payload=payload,
            )
            return msg_id if result is not None else None
        except Exception as exc:
            log_callback(f"send failed: {exc}", "ERROR")
            return False

    rest = RESTServer(
        config, message_handler, log_callback, kafka_broker,
        message_logger=logger,
    )
    rest.start()

    log_callback(
        f"REST listening on {config['rest_api']['host']}:{config['rest_api']['port']}",
        "INFO",
    )

    stop = {"flag": False}

    def _sig(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    try:
        while not stop["flag"]:
            time.sleep(1)
    finally:
        log_callback("Shutting down…", "INFO")
        if rest:
            rest.stop()
        if xmpp_service:
            xmpp_service.disconnect()
        if kafka_broker:
            kafka_broker.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
