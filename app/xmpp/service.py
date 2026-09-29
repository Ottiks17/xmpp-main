
"""XMPP-сервис: по умолчанию нативный клиент (стабилен для локальных серверов)."""

from __future__ import annotations

from typing import Callable, Optional

from app.validation import validate_jid
from app.xmpp.native import SimpleXMPPClient

try:
    from app.xmpp.slixmpp_backend import SlixmppClient
except ImportError:
    SlixmppClient = None


class XMPPService:
    def __init__(self, config: dict, log_callback: Optional[Callable[[str, str], None]] = None, kafka_broker=None):
        self.config = config
        self.log_callback = log_callback
        self.kafka_broker = kafka_broker
        self.client: Optional[SimpleXMPPClient] = None

    def connect(self, username: str, password: str) -> bool:
        try:
            xmpp_cfg = self.config.get("xmpp", {})
            server = xmpp_cfg.get("server", "")
            if "@" in username:
                jid = username
            else:
                jid = f"{username}@{server}"

            env_password = password or xmpp_cfg.get("password") or ""
            use_slixmpp = bool(xmpp_cfg.get("use_slixmpp", False))

            # Создаем callback для публикации в Kafka
            kafka_publisher = None
            if self.kafka_broker and self.config.get("kafka", {}).get("publish_all_messages", False):
                kafka_publisher = self._create_kafka_publisher()

            if use_slixmpp and SlixmppClient is not None:
                self.client = SlixmppClient(
                    jid,
                    env_password,
                    server,
                    int(xmpp_cfg.get("port", 5222)),
                    bool(xmpp_cfg.get("use_tls", True)),
                    bool(xmpp_cfg.get("verify_tls", True)),
                    self.log_callback,
                    kafka_publisher=kafka_publisher,
                )
            else:
                self.client = SimpleXMPPClient(
                    jid,
                    env_password,
                    server,
                    int(xmpp_cfg.get("port", 5222)),
                    self.log_callback,
                    use_tls=bool(xmpp_cfg.get("use_tls", True)),
                    verify_tls=bool(xmpp_cfg.get("verify_tls", True)),
                    kafka_publisher=kafka_publisher,
                )

            return self.client.connect()
        except Exception as exc:
            if self.log_callback:
                self.log_callback(f"Connection failed: {exc}", "ERROR")
            return False

    def _create_kafka_publisher(self):
        """Создает callback для публикации сообщений в Kafka."""
        def publish_to_kafka(msg_type: str, sender: str, recipient: str, message: str):
            if not self.kafka_broker:
                return
            try:
                kafka_config = self.config.get("kafka", {})
                topic = kafka_config.get("topics", {}).get("messages", "xmpp-messages")
                
                msg_data = {
                    "type": msg_type,
                    "sender": sender,
                    "recipient": recipient,
                    "message": message,
                    "timestamp": None,  # Заполнится в native.py
                }
                self.kafka_broker.publish(topic, msg_data)
            except Exception as exc:
                if self.log_callback:
                    self.log_callback(f"Kafka publish error: {exc}", "ERROR")
        
        return publish_to_kafka

    def send_message(
        self,
        to_username: str,
        message: str,
        msg_id: Optional[str] = None,
        priority: Optional[str] = None,
    ):
        if not self.client or not self.client.is_connected:
            raise RuntimeError("Not connected to XMPP server")
        domain = self.config["xmpp"]["server"]
        to_jid = validate_jid(to_username, domain)
        if isinstance(self.client, SimpleXMPPClient):
            return self.client.queue_message(to_jid, message, msg_id, priority)
        # slixmpp-бэкенд ещё не знает про priority: отправляем как обычное сообщение
        if priority and priority != "normal" and self.log_callback:
            self.log_callback("slixmpp-бэкенд не поддерживает priority — отправлено как normal", "WARNING")
        return self.client.queue_message(to_jid, message, msg_id)

    def register_user(self, username: str, password: str, email: Optional[str] = None) -> bool:
        if not self.client or not self.client.is_connected:
            raise RuntimeError("Not connected to XMPP server")
        return self.client.register_user(username, password, email)

    def send_subscription_request(self, to_username: str) -> bool:
        if not self.client or not self.client.is_connected:
            raise RuntimeError("Not connected to XMPP server")
        domain = self.config["xmpp"]["server"]
        to_jid = validate_jid(to_username, domain)
        return self.client.send_subscription_request(to_jid)

    def disconnect(self) -> None:
        if self.client:
            self.client.disconnect()



