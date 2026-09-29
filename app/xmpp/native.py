
"""Нативный XMPP-клиент (сокеты) — совместим с локальными серверами."""

import base64
import json
import random
import re
import socket
import ssl
import string
import threading
import time
import uuid
from datetime import datetime
from queue import Empty, Queue

from app.validation import normalize_priority

# Расширение с приоритетом сообщения (см. stage1_contract.md)
PRIORITY_NS = "urn:wakeup:msg:0"


def _escape_xml(text):
    if text is None:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


class SimpleXMPPClient:
    def __init__(
        self,
        jid,
        password,
        server,
        port=5222,
        log_callback=None,
        use_tls=True,
        verify_tls=True,
        kafka_publisher=None,
    ):
        self.jid = jid
        self.password = password
        self.server = server
        self.port = port
        self.log_callback = log_callback
        self.use_tls = use_tls
        self.verify_tls = verify_tls
        self.kafka_publisher = kafka_publisher
        self.socket = None
        self.is_connected = False
        self.message_queue = Queue()
        self.running = True
        self.resource = None
        self.full_jid = None
        self.on_message_received = None
        self.on_delivery_received = None
        self.on_read_received = None
        self.on_delivery_received = None
        self.on_read_received = None
        self.on_send_failed = None

    def connect(self):
        """Establish connection to XMPP server with TLS support.
        
        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            self.running = True
            self._log(f"Подключение к {self.server}:{self.port}...", "INFO")

            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(15)
            self.socket.connect((self.server, self.port))

            self._send_raw('<?xml version="1.0"?>')
            self._send_raw(
                f'<stream:stream to="{self.server}" xmlns="jabber:client" '
                f'xmlns:stream="http://etherx.jabber.org/streams" version="1.0">'
            )

            time.sleep(0.5)
            response = self._receive_data()

            if self.use_tls and "<starttls" in response:
                self._log("STARTTLS...", "INFO")
                self._send_raw('<starttls xmlns="urn:ietf:params:xml:ns:xmpp-tls"/>')
                time.sleep(0.5)
                response = self._receive_data()

                if "<proceed" in response:
                    context = ssl.create_default_context()
                    if self.verify_tls:
                        context.check_hostname = True
                        context.verify_mode = ssl.CERT_REQUIRED
                    else:
                        context.check_hostname = False
                        context.verify_mode = ssl.CERT_NONE
                    self.socket = context.wrap_socket(
                        self.socket, server_hostname=self.server
                    )
                    self._send_raw(
                        f'<stream:stream to="{self.server}" xmlns="jabber:client" '
                        f'xmlns:stream="http://etherx.jabber.org/streams" version="1.0">'
                    )
                    time.sleep(0.5)
                    self._receive_data()

            return self._auth_plain()
        except Exception as exc:
            self._log(f"Ошибка подключения: {exc}", "ERROR")
            return False

    def _auth_plain(self):
        try:
            password = self.password
            self.password = None
            auth_string = "\x00" + self.jid.split("@")[0] + "\x00" + password
            auth_encoded = base64.b64encode(auth_string.encode()).decode()
            self._send_raw(
                f"<auth xmlns=\"urn:ietf:params:xml:ns:xmpp-sasl\" mechanism=\"PLAIN\">{auth_encoded}</auth>"
            )
            time.sleep(1)
            response = self._receive_data()

            if "<success" not in response:
                self._log(f"Auth failed: {response[:120]}", "ERROR")
                return False

            self._finalize_connection()
            return True
        except Exception as exc:
            self._log(f"Auth error: {exc}", "ERROR")
            return False
    def _finalize_connection(self):
        self.resource = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        self._send_raw(
            f'<stream:stream to="{self.server}" xmlns="jabber:client" '
            f'xmlns:stream="http://etherx.jabber.org/streams" version="1.0">'
        )
        time.sleep(0.5)

        bind_iq = (
            f'<iq type="set" id="bind_1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind">'
            f"<resource>{self.resource}</resource></bind></iq>"
        )
        self._send_raw(bind_iq)
        time.sleep(0.5)

        response = self._receive_data()
        jid_match = re.search(r'jid="([^"]+)"', response)
        self.full_jid = jid_match.group(1) if jid_match else f"{self.jid}/{self.resource}"

        self._send_raw(
            '<presence>'
            '<priority>100</priority>'
            '<c xmlns="http://jabber.org/protocol/caps"'
            ' node="https://xmpp-client.local"'
            ' hash="sha-1"'
            ' ver="FiFjl04QgdFstczk4I8I9wampPY="/>'
            '</presence>'
        )
        self.is_connected = True
        self._log(f"Подключено: {self.full_jid}", "INFO")

        threading.Thread(target=self._process_queue, daemon=True).start()
        threading.Thread(target=self._read_loop, daemon=True).start()

    def register_user(self, username, password, email=None):
        if not self.is_connected:
            return False
        try:
            user = username.split("@")[0] if "@" in username else username
            fields = (
                f"<username>{_escape_xml(user)}</username>"
                f"<password>{_escape_xml(password)}</password>"
            )
            if email:
                fields += f"<email>{_escape_xml(email)}</email>"
            reg_id = f"reg_{int(time.time() * 1000)}"
            self._send_raw(
                f'<iq type="set" id="{reg_id}">'
                f'<query xmlns="jabber:iq:register">{fields}</query></iq>'
            )
            time.sleep(1.5)
            response = self._receive_data()
            return 'type="result"' in response
        except Exception as exc:
            self._log(f"Ошибка регистрации: {exc}", "ERROR")
            return False

    def send_subscription_request(self, to_jid):
        if not self.is_connected:
            return False
        try:
            self._send_raw(f'<presence to="{_escape_xml(to_jid)}" type="subscribe"/>')
            return True
        except Exception as exc:
            self._log(f"Ошибка подписки: {exc}", "ERROR")
            return False

    def send_displayed_marker(self, to_jid, message_id):
        if not self.is_connected or not message_id:
            return
        self._send_raw(
            f'<message to="{_escape_xml(to_jid)}" type="chat">'
            f'<displayed xmlns="urn:xmpp:chat-markers:0" id="{_escape_xml(message_id)}"/>'
            f"</message>"
        )

    def _receive_data(self):
        try:
            return self.socket.recv(8192).decode("utf-8", errors="ignore")
        except OSError:
            return ""

    def _send_raw(self, data):
        self.socket.send(f"{data}\n".encode("utf-8"))

    def reconnect(self):
        self.disconnect()
        self.running = True
        time.sleep(1)
        return self.connect()

    def send_message(self, to, message, msg_id=None, priority=None):
        """Send XMPP message with delivery receipts.
        
        Args:
            to (str): Recipient JID
            message (str): Message text (max 256 chars)
            msg_id (str, optional): Message ID. Defaults to uuid4.
            priority (str, optional): low/normal/high/critical. Defaults to normal.
        
        Returns:
            str: Message ID if successful, None otherwise.
        """
        if not self.is_connected:
            if not self.reconnect():
                return None

        try:
            if len(message) > 256:
                raise ValueError("Message exceeds 256 characters")
            escaped = _escape_xml(message)
            priority = normalize_priority(priority)
            if msg_id is None:
                msg_id = str(uuid.uuid4())

            msg_xml = (
                f'<message to="{_escape_xml(to)}" type="chat" id="{_escape_xml(msg_id)}">'
                f"<body>{escaped}</body>"
                f'<request xmlns="urn:xmpp:receipts"/>'
                f'<markable xmlns="urn:xmpp:chat-markers:0"/>'
                f'<priority xmlns="{PRIORITY_NS}" level="{priority}"/>'
                f"</message>"
            )
            self._send_raw(msg_xml)
            self._log_message(
                type="SENT",
                sender=self.full_jid or self.jid,
                recipient=to,
                message=message,
                message_id=msg_id,
                priority=priority,
                send_time=datetime.now(),
            )
            
            # Публикация в Kafka
            if self.kafka_publisher:
                try:
                    self.kafka_publisher(
                        "SENT",
                        self.full_jid or self.jid,
                        to,
                        message
                    )
                except Exception as exc:
                    self._log(f"Kafka publisher error: {exc}", "ERROR")
            
            return msg_id
        except Exception as exc:
            self._log(f"Ошибка отправки: {exc}", "ERROR")
            return None

    def queue_message(self, to, message, msg_id=None, priority=None):
        """Ставит сообщение в очередь и возвращает его ID (uuid4, если не задан)."""
        msg_id = msg_id or str(uuid.uuid4())
        self.message_queue.put((to, message, msg_id, priority))
        return msg_id

    def _process_queue(self):
        while self.running and self.is_connected:
            try:
                to, message, msg_id, priority = self.message_queue.get(timeout=1)
                if self.send_message(to, message, msg_id, priority) is None and self.on_send_failed:
                    self.on_send_failed(msg_id)
            except Empty:
                continue
            except Exception as exc:
                self._log(f"Очередь: {exc}", "ERROR")

    def _read_loop(self):
        buffer = ""
        while self.running and self.is_connected:
            try:
                data = self.socket.recv(4096).decode("utf-8", errors="ignore")
                if not data:
                    time.sleep(0.1)
                    continue
                buffer += data
                raw_str = data if isinstance(data, str) else data.decode('utf-8', errors='ignore')
                if 'displayed' in raw_str or ('received' in raw_str and 'urn:xmpp:receipts' in raw_str):
                    self._log(f"RAW_MARKER: {raw_str[:800]}", "INFO")

                while "<message" in buffer and "</message>" in buffer:
                    start = buffer.find("<message")
                    end = buffer.find("</message>") + 10
                    if start != -1 and end > start:
                        self._process_incoming_message(buffer[start:end])
                        buffer = buffer[end:]

                while "<received" in buffer and 'id="' in buffer:
                    start = buffer.find("<received")
                    end = buffer.find("/>", start) + 2
                    if start != -1 and end > start:
                        self._process_delivery(buffer[start:end])
                        buffer = buffer[end:]

                while "<displayed" in buffer and 'id="' in buffer:
                    start = buffer.find("<displayed")
                    end = buffer.find("/>", start) + 2
                    if start != -1 and end > start:
                        self._process_read(buffer[start:end])
                        buffer = buffer[end:]

                # Handle IQ requests (version, disco)
                if '<iq' in buffer and 'type="get"' in buffer:
                    iq_start = buffer.find('<iq')
                    iq_end = buffer.find('</iq>', iq_start)
                    if iq_end == -1:
                        iq_end = buffer.find('/>', iq_start)
                    if iq_start != -1 and iq_end > iq_start:
                        iq_end += 5 if '</iq>' in buffer[iq_start:] else 2
                        iq_xml = buffer[iq_start:iq_end]
                        buffer = buffer[iq_end:]
                        iq_id_m = re.search(r'id="([^"]+)"', iq_xml)
                        iq_from_m = re.search(r'from="([^"]+)"', iq_xml)
                        iq_id = iq_id_m.group(1) if iq_id_m else 'iq1'
                        iq_from = iq_from_m.group(1) if iq_from_m else self.server
                        if 'jabber:iq:version' in iq_xml:
                            self._send_raw(
                                f'<iq type="result" id="{_escape_xml(iq_id)}" to="{_escape_xml(iq_from)}">'
                                f'<query xmlns="jabber:iq:version">'
                                f'<name>XMPP Client</name>'
                                f'<version>1.0</version>'
                                f'</query></iq>'
                            )
                        elif 'http://jabber.org/protocol/disco#info' in iq_xml:
                            self._send_raw(
                                f'<iq type="result" id="{iq_id}" to="{iq_from}">'
                                f'<query xmlns="http://jabber.org/protocol/disco#info" node="https://xmpp-client.local#FiFjl04QgdFstczk4I8I9wampPY=">'
                                f'<identity category="client" type="pc" name="XMPP Client"/>'
                                f'<feature var="jabber:iq:version"/>'
                                f'<feature var="urn:xmpp:receipts"/>'
                                f'<feature var="urn:xmpp:chat-markers:0"/>'
                                f'<feature var="http://jabber.org/protocol/chatstates"/>'
                                f'</query></iq>'
                            )
                if '<presence' in buffer and 'type="subscribe"' in buffer:
                    start = buffer.find("<presence")
                    end = buffer.find("/>", start) + 2
                    if start != -1 and end > start:
                        sub = buffer[start:end]
                        buffer = buffer[end:]
                        from_m = re.search(r'from="([^"]+)"', sub)
                        if from_m:
                            jid = from_m.group(1).split("/")[0]
                            self._send_raw(f'<presence to="{_escape_xml(jid)}" type="subscribed"/>')

                if len(buffer) > 12000:
                    buffer = buffer[-2000:]
            except socket.timeout:
                continue
            except Exception as exc:
                if self.running:
                    self._log(f"Чтение: {exc}", "ERROR")
                break

    def _process_incoming_message(self, msg_xml):
        from_m = re.search(r'from="([^"]+)"', msg_xml)
        body_m = re.search(r"<body>(.*?)</body>", msg_xml)
        id_m = re.search(r'id="([^"]+)"', msg_xml)
        # Handle <received> receipt inside <message> (XEP-0184)
        if from_m and not body_m:
            # Check for delivery receipt
            recv_m = re.search(r'<received[^>]*/>', msg_xml)
            if not recv_m:
                recv_m = re.search(r'<received[^>]*></received>', msg_xml)
            if recv_m:
                recv_id_m = re.search(r'id="([^"]+)"', recv_m.group(0))
                if recv_id_m and self.on_delivery_received:
                    self.on_delivery_received(recv_id_m.group(1))
            # Check for displayed marker
            disp_m = re.search(r'<displayed[^>]*/>', msg_xml)
            if not disp_m:
                disp_m = re.search(r'<displayed[^>]*></displayed>', msg_xml)
            if disp_m:
                disp_id_m = re.search(r'id="([^"]+)"', disp_m.group(0))
                if disp_id_m and self.on_read_received:
                    self.on_read_received(disp_id_m.group(1))
            return

        from_jid = from_m.group(1).split("/")[0]
        body = body_m.group(1)
        body = (
            body.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", '"')
            .replace("&amp;", "&")
        )
        incoming_id = id_m.group(1) if id_m else ""

        self._log_message(
            type="RECEIVED",
            sender=from_jid,
            recipient=self.jid,
            message=body,
            message_id=incoming_id,
            delivery_time=datetime.now(),
        )

        # Публикация входящего сообщения в Kafka
        if self.kafka_publisher:
            try:
                self.kafka_publisher(
                    "RECEIVED",
                    from_jid,
                    self.jid,
                    body
                )
            except Exception as exc:
                self._log(f"Kafka publisher error: {exc}", "ERROR")

        if incoming_id:
            self._send_raw(
                f'<message to="{_escape_xml(from_jid)}" type="chat">'
                f'<received xmlns="urn:xmpp:receipts" id="{_escape_xml(incoming_id)}"/>'
                f"</message>"
            )

        if self.on_message_received:
            self.on_message_received(from_jid, body, incoming_id or None)

    def _process_delivery(self, xml_chunk):
        id_m = re.search(r'id="([^"]+)"', xml_chunk)
        if id_m and self.on_delivery_received:
            self.on_delivery_received(id_m.group(1))

    def _process_read(self, xml_chunk):
        id_m = re.search(r'id="([^"]+)"', xml_chunk)
        if id_m and self.on_read_received:
            self.on_read_received(id_m.group(1))

    def _log(self, message, level="INFO"):
        if self.log_callback:
            self.log_callback(message, level)

    def _log_message(self, **fields):
        payload = {
            "type": fields.get("type"),
            "sender": fields.get("sender"),
            "recipient": fields.get("recipient"),
            "message": fields.get("message"),
            "message_id": fields.get("message_id"),
            "priority": fields.get("priority"),
            "send_time": fields["send_time"].isoformat()
            if fields.get("send_time")
            else None,
            "delivery_time": fields["delivery_time"].isoformat()
            if fields.get("delivery_time")
            else None,
            "read_time": None,
            "timestamp": datetime.now().isoformat(),
        }
        if self.log_callback:
            self.log_callback(json.dumps(payload, ensure_ascii=False), "MESSAGE")

    def disconnect(self):
        self.running = False
        self.is_connected = False
        if self.socket:
            try:
                self.socket.close()
            except OSError:
                pass
        self._log("Отключено", "INFO")




