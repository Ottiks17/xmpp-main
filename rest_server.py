from flask import Flask, request, jsonify
from threading import Thread
import hmac
import logging
from datetime import datetime
import json
import sqlite3
import os
import re
import uuid
from collections import defaultdict, deque
from time import time

from app.constants import MAX_MESSAGE_LENGTH, MESSAGES_DB_PATH
from app.validation import (
    default_body, normalize_message_type, normalize_priority, validate_external_id,
    validate_message, validate_payload,
)
from app.devices import DeviceRegistry, validate_device_id
from app.kafka_broker import KafkaBroker
from app.storage import MessageLogger

# ID сообщения: uuid4 или старые числовые ID
_MESSAGE_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
# Поля, которые отдаёт GET /messages/{id}
_STATUS_FIELDS = ("message_id", "recipient", "priority", "status", "send_time", "delivery_time", "read_time")
_TO_FIELDS = ("to", "recipient", "user", "username", "jid", "send_to", "target")
_TEXT_FIELDS = ("message", "msg", "text", "body", "content")
# Служебные поля, которые не попадают в текст при устаревшем режиме «тело запроса как сообщение»
_SERVICE_FIELDS = ("priority", "external_id", "type", "payload")
MAX_STATUS_BATCH = 100
DEFAULT_BROADCAST_MAX_RECIPIENTS = 200

# Описание новых эндпоинтов для /swagger.json (контракт v1.0)
_EXTRA_SWAGGER_PATHS = {
    "/messages/status": {"post": {"summary": "Пакетный статус сообщений (до 100 message_id)",
                                   "responses": {"200": {"description": "Статусы и not_found"}}}},
    "/v1/auth/xmpp": {"get": {"summary": "XMPP-учётные данные терминала по deviceId",
                               "responses": {"200": {"description": "xmppHost, xmppPort, xmppLogin, xmppPassword, apiHost, apiPort"},
                                             "404": {"description": "Терминал не зарегистрирован"}}}},
    "/v1/wakeup/confirm": {"post": {"summary": "Подтверждение пробуждения WMS (deviceId, taskId)",
                                     "responses": {"200": {"description": "Принято"}}}},
    "/v1/config/trusted-senders": {"get": {"summary": "Доверенные отправители для критичных сообщений",
                                            "responses": {"200": {"description": "{\"senders\": [...]}"}}}},
    "/v1/admin/devices": {"get": {"summary": "Реестр терминалов (нужен api_key)",
                                   "responses": {"200": {"description": "Список без паролей"}}}},
    "/v1/admin/devices/{device_id}": {"put": {"summary": "Создать/обновить терминал и привязку XMPP-аккаунта",
                                               "responses": {"200": {"description": "Запись без секретов"}}},
                                        "delete": {"summary": "Удалить терминал",
                                                   "responses": {"200": {"description": "Удалён"}}}},
}


def normalize_message_type_safe(value):
    """Тип сообщения для ветвления в разборе запроса; неверное значение отдаёт ошибку позже, в _prepare."""
    try:
        return normalize_message_type(value)
    except ValueError:
        return "text"


class RateLimiter:
    """Simple rate limiter by IP address (requests per minute)."""
    def __init__(self, max_requests=100, window_seconds=60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = defaultdict(deque)

    def is_allowed(self, client_ip: str) -> bool:
        """Check if client can make a request."""
        now = time()
        requests = self.requests[client_ip]
        
        # Remove old requests outside the window
        while requests and requests[0] < now - self.window_seconds:
            requests.popleft()
        
        # Check limit
        if len(requests) >= self.max_requests:
            return False
        
        requests.append(now)
        return True

class RESTServer:
    def __init__(self, config, message_handler=None, log_callback=None, kafka_broker=None, message_logger=None):
        self.config = config
        # message_handler(to_user, message, priority, msg_id) -> truthy при успехе
        self.message_handler = message_handler
        # MessageLogger: если задан, каждое сообщение сразу пишется в БД со статусом queued
        self.message_logger = message_logger
        self.log_callback = log_callback
        self.app = Flask(__name__)
        self.server_thread = None
        self.is_running = False
        self._swagger_enabled = False
        self.kafka_broker = kafka_broker
        self.rate_limiter = RateLimiter(max_requests=100, window_seconds=60)
        self.device_registry = None  # создаётся лениво на той же БД, что и история сообщений

        # Инициализация Kafka (если включена и не передан внешний брокер)
        if not self.kafka_broker and self.config.get("kafka", {}).get("enabled", False):
            self._init_kafka()

        # Настройка Swagger
        self._setup_swagger()
        self._setup_routes()
        
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)
    
    def _rest_cfg(self):
        return self.config.get("rest_api", {})

    @staticmethod
    def _err(code, message, http_status, **extra):
        """Единый формат ошибки контракта v1.0: {"error": {"code", "message"}} (+ доп. поля рядом)."""
        body = {"error": {"code": code, "message": message}}
        body.update(extra)
        return jsonify(body), http_status

    def _route(self, path, **options):
        """Регистрирует маршрут и его алиас под /v1."""
        def decorator(func):
            self.app.add_url_rule(path, endpoint=func.__name__, view_func=func, **options)
            self.app.add_url_rule("/v1" + path, endpoint=func.__name__ + "_v1", view_func=func, **options)
            return func
        return decorator

    def _check_api_key(self):
        """Ключ принимается заголовком X-API-Key; query-параметр api_key — только если
        rest_api.allow_api_key_query = true (ключ в URL попадает в логи и историю прокси)."""
        cfg = self._rest_cfg()
        api_key = cfg.get("api_key", "")
        if not api_key:
            return None
        provided = request.headers.get("X-API-Key", "")
        if not provided and cfg.get("allow_api_key_query", False):
            provided = request.args.get("api_key", "")
        if not hmac.compare_digest(provided.encode("utf-8"), api_key.encode("utf-8")):
            hint = "Provide X-API-Key header" + (" or api_key query param" if cfg.get("allow_api_key_query", False) else "")
            return self._err("unauthorized", f"Unauthorized. {hint}.", 401)
        return None

    def _require_admin(self):
        """Административный API работает только при заданном api_key."""
        if not self._rest_cfg().get("api_key", ""):
            return self._err("admin_disabled", "Admin API requires rest_api.api_key to be set", 403)
        return self._check_api_key()

    def _rate_limited(self):
        client_ip = request.remote_addr
        if not self.rate_limiter.is_allowed(client_ip):
            self._log(f"Rate limit exceeded for {client_ip}", "WARNING")
            return self._err("rate_limited", "Too many requests. Rate limit exceeded.", 429)
        return None

    def _registry(self):
        if self.device_registry is None:
            self.device_registry = DeviceRegistry(db_path=self._get_logger().db_path)
        return self.device_registry

    def _prepare(self, message, raw_priority, raw_type, raw_payload, raw_external_id):
        """Общая валидация send/broadcast. Бросает ValueError с текстом для клиента."""
        msg_type = normalize_message_type(raw_type)
        payload = validate_payload(msg_type, raw_payload)
        if message in (None, "") and msg_type != "text":
            message = default_body(msg_type, payload)
        return {
            "message": validate_message(str(message) if message is not None else ""),
            "priority": normalize_priority(raw_priority),
            "type": msg_type,
            "payload": payload,
            "external_id": validate_external_id(raw_external_id),
        }

    def _call_handler(self, to_user, prepared, msg_id):
        """Для type=text — прежний вызов из 4 аргументов; для task/command добавляются type и payload."""
        if prepared["type"] == "text":
            return self.message_handler(to_user, prepared["message"], prepared["priority"], msg_id)
        return self.message_handler(
            to_user, prepared["message"], prepared["priority"], msg_id, prepared["type"], prepared["payload"]
        )

    def _find_duplicate(self, external_id):
        if not external_id or not self.message_logger:
            return None
        return self.message_logger.find_by_external_id(external_id)

    @staticmethod
    def _duplicate_body(row):
        import json as _json
        body = {
            "status": "success",
            "duplicate": True,
            "message_id": row["message_id"],
            "priority": row["priority"],
            "message_status": row["status"],
            "type": row.get("content_type") or "text",
            "to": row["recipient"],
            "message": row["message"],
        }
        if row.get("external_id"):
            body["external_id"] = row["external_id"]
        if row.get("payload"):
            body["payload"] = _json.loads(row["payload"])
        return body

    def _get_logger(self):
        """MessageLogger для чтения. Если не передан в конструктор, создаётся по пути из констант."""
        if self.message_logger is None:
            self.message_logger = MessageLogger(db_path=MESSAGES_DB_PATH)
        return self.message_logger

    def _record_queued(self, msg_id, to_user, message, priority, content_type="text",
                       payload=None, external_id=None):
        """Синхронно создаёт строку queued, чтобы GET /messages/{id} видел сообщение сразу.

        Возвращает "ok", "duplicate" (external_id уже занят — гонка двух одинаковых запросов)
        или "error"."""
        if not self.message_logger:
            return "ok"
        try:
            self.message_logger.log_message(
                "SENT", "api", to_user, message,
                message_id=msg_id, priority=priority, status="queued",
                external_id=external_id, content_type=content_type, payload=payload,
            )
            return "ok"
        except sqlite3.IntegrityError:
            return "duplicate"
        except Exception as exc:
            self._log(f"Не удалось записать queued {msg_id}: {exc}", "WARNING")
            return "error"

    def _mark_failed(self, msg_id):
        if not self.message_logger:
            return
        try:
            self.message_logger.mark_failed(msg_id)
        except Exception as exc:
            self._log(f"Не удалось пометить failed {msg_id}: {exc}", "WARNING")

    def _init_kafka(self):
        """Инициализация Kafka брокера."""
        try:
            kafka_config = self.config.get("kafka", {})
            bootstrap_servers = kafka_config.get("bootstrap_servers", "localhost:9092")
            
            self.kafka_broker = KafkaBroker(bootstrap_servers, self._log)
            if self.kafka_broker.connect():
                self._log("Kafka брокер инициализирован", "INFO")
            else:
                self._log("Ошибка подключения к Kafka, отключение", "ERROR")
                self.kafka_broker = None
        except Exception as exc:
            self._log(f"Ошибка инициализации Kafka: {exc}", "ERROR")
            self.kafka_broker = None

    def _publish_to_kafka(self, topic: str, message: dict):
        """Публикация сообщения в Kafka топик."""
        if not self.kafka_broker:
            return False
        try:
            return self.kafka_broker.publish(topic, message)
        except Exception as exc:
            self._log(f"Ошибка публикации в Kafka: {exc}", "ERROR")
            return False

    def _process_kafka_api_request(self, topic: str, message: dict):
        """Обработчик сообщений из Kafka для API-очереди."""
        if not self.message_handler:
            self._log("Message handler not configured for Kafka consumer", "ERROR")
            return

        try:
            to_user = message.get("to")
            text = message.get("message")
            if not to_user or not text:
                self._log(f"Неправильное сообщение из Kafka: {message}", "ERROR")
                return

            try:
                priority = normalize_priority(message.get("priority"))
            except ValueError:
                self._log(f"Неизвестный priority в Kafka-сообщении, используется normal: {message.get('priority')}", "WARNING")
                priority = "normal"
            try:
                prepared = self._prepare(
                    text, priority, message.get("type"), message.get("payload"), message.get("external_id")
                )
            except ValueError as exc:
                self._log(f"Некорректное Kafka-сообщение: {exc}", "ERROR")
                if message.get("message_id"):
                    self._mark_failed(message["message_id"])
                return
            msg_id = message.get("message_id")
            if not msg_id:
                # запись положил внешний продюсер напрямую в топик
                msg_id = str(uuid.uuid4())
                if self._record_queued(msg_id, to_user, prepared["message"], prepared["priority"],
                                       prepared["type"], prepared["payload"], prepared["external_id"]) == "duplicate":
                    self._log("Kafka: повторный external_id, сообщение пропущено", "WARNING")
                    return

            self._log(f"Обработка Kafka API-запроса для {to_user}", "INFO")
            success = self._call_handler(to_user, prepared, msg_id)
            if success:
                self._publish_message_event("SENT", to_user, text)
            else:
                self._mark_failed(msg_id)
                self._log(f"Kafka API message handler failed for {to_user}", "ERROR")
        except Exception as exc:
            self._log(f"Ошибка обработки Kafka сообщения: {exc}", "ERROR")

    def _publish_message_event(self, event_type: str, recipient: str, message: str):
        """Публикация события отправленного сообщения в Kafka."""
        kafka_config = self.config.get("kafka", {})
        if not kafka_config.get("enabled", False):
            return False

        topic = kafka_config.get("topics", {}).get("messages", "xmpp-messages")
        payload = {
            "type": event_type,
            "recipient": recipient,
            "message": message,
            "timestamp": datetime.now().isoformat(),
        }
        return self._publish_to_kafka(topic, payload)

    def _setup_kafka_consumers(self):
        """Запуск Kafka consumer для обработки API-очереди."""
        kafka_config = self.config.get("kafka", {})
        if not self.kafka_broker:
            return
        if not kafka_config.get("consume_api_requests", False):
            return
        if not self.message_handler:
            self._log("Message handler не задан — Kafka consumer не будет запущен", "WARNING")
            return

        topics = [kafka_config.get("topics", {}).get("api_requests", "xmpp-api-requests")]
        group = kafka_config.get("consumer_group", "xmpp-client")
        offset = kafka_config.get("auto_offset_reset", "latest")
        success = self.kafka_broker.subscribe(topics, group, self._process_kafka_api_request, auto_offset_reset=offset)
        if success:
            self._log(f"Kafka consumer запущен для топиков: {topics}", "INFO")

    def _setup_swagger(self):
        """Настройка Swagger документации (опционально)"""
        try:
            from flask_swagger_ui import get_swaggerui_blueprint
            from flasgger import Swagger
        except ImportError:
            self._log("Swagger не установлен — документация API отключена", "WARNING")
            return

        self.app.config['SWAGGER'] = {
            'title': 'XMPP Messenger API',
            'version': '2.0',
            'description': 'API для отправки сообщений через XMPP протокол',
            'contact': {
                'name': 'Support',
                'email': 'support@xmpp.local'
            },
            'license': {
                'name': 'MIT',
                'url': 'https://opensource.org/licenses/MIT'
            }
        }
        
        # Swagger UI
        swagger_url = '/apidocs'
        api_url = '/swagger.json'
        
        swagger_ui_blueprint = get_swaggerui_blueprint(
            swagger_url,
            api_url,
            config={
                'app_name': "XMPP Messenger API"
            }
        )
        self.app.register_blueprint(swagger_ui_blueprint, url_prefix=swagger_url)
        
        self.swagger = Swagger(self.app)
        self._swagger_enabled = True

    def _log(self, message, level="INFO"):
        if self.log_callback:
            self.log_callback(f"[REST API] {message}", level)
        else:
            print(f"[{level}] {message}")
    
    def _setup_routes(self):
        
        @self.app.route('/swagger.json')
        def swagger_json():
            spec = ({
                "swagger": "2.0",
                "info": {
                    "title": "XMPP Messenger API",
                    "version": "2.0",
                    "description": "API для отправки сообщений через XMPP протокол"
                },
                "basePath": "/",
                "schemes": ["http"],
                "paths": {
                    "/send_message": {
                        "get": {
                            "summary": "Отправить сообщение (GET)",
                            "description": "Отправка сообщения через GET параметры",
                            "parameters": [
                                {
                                    "name": "to",
                                    "in": "query",
                                    "required": True,
                                    "type": "string",
                                    "description": "JID получателя (user@arsenal)"
                                },
                                {
                                    "name": "message",
                                    "in": "query",
                                    "required": True,
                                    "type": "string",
                                    "maxLength": 256,
                                    "description": "Текст сообщения (до 256 символов)"
                                },
                                {
                                    "name": "priority",
                                    "in": "query",
                                    "required": False,
                                    "type": "string",
                                    "enum": ["low", "normal", "high", "critical"],
                                    "default": "normal",
                                    "description": "Приоритет сообщения"
                                }
                            ],
                            "responses": {
                                "200": {"description": "Сообщение отправлено"},
                                "400": {"description": "Ошибка валидации"},
                                "500": {"description": "Внутренняя ошибка"}
                            }
                        },
                        "post": {
                            "summary": "Отправить сообщение (POST)",
                            "description": "Отправка сообщения через JSON",
                            "parameters": [
                                {
                                    "name": "body",
                                    "in": "body",
                                    "required": True,
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "to": {
                                                "type": "string",
                                                "example": "user@arsenal",
                                                "description": "JID получателя"
                                            },
                                            "message": {
                                                "type": "string",
                                                "maxLength": 256,
                                                "example": "Hello!",
                                                "description": "Текст сообщения"
                                            },
                                            "priority": {
                                                "type": "string",
                                                "enum": ["low", "normal", "high", "critical"],
                                                "default": "normal",
                                                "description": "Приоритет сообщения"
                                            }
                                        }
                                    }
                                }
                            ],
                            "responses": {
                                "200": {"description": "Сообщение отправлено"},
                                "400": {"description": "Ошибка валидации"},
                                "500": {"description": "Внутренняя ошибка"}
                            }
                        }
                    },
                    "/health": {
                        "get": {
                            "summary": "Проверка работоспособности",
                            "responses": {
                                "200": {"description": "API работает"}
                            }
                        }
                    },
                    "/stats": {
                        "get": {
                            "summary": "Статистика сообщений",
                            "responses": {
                                "200": {"description": "Статистика успешно получена"}
                            }
                        }
                    },
                    "/get_messages": {
                        "get": {
                            "summary": "Получить последние сообщения",
                            "parameters": [
                                {
                                    "name": "limit",
                                    "in": "query",
                                    "type": "integer",
                                    "default": 100,
                                    "description": "Количество сообщений (1-1000)"
                                },
                                {
                                    "name": "type",
                                    "in": "query",
                                    "type": "string",
                                    "enum": ["SENT", "RECEIVED"],
                                    "description": "Фильтр по типу"
                                },
                                {
                                    "name": "status",
                                    "in": "query",
                                    "type": "string",
                                    "enum": ["queued", "sent", "delivered", "read", "failed"],
                                    "description": "Фильтр по статусу"
                                },
                                {
                                    "name": "priority",
                                    "in": "query",
                                    "type": "string",
                                    "enum": ["low", "normal", "high", "critical"],
                                    "description": "Фильтр по приоритету"
                                }
                            ],
                            "responses": {
                                "200": {"description": "Сообщения успешно получены"}
                            }
                        }
                    },
                    "/messages/{message_id}": {
                        "get": {
                            "summary": "Статус сообщения по ID",
                            "description": "queued -> sent -> delivered -> read, либо failed",
                            "parameters": [
                                {
                                    "name": "message_id",
                                    "in": "path",
                                    "required": True,
                                    "type": "string",
                                    "description": "message_id из ответа /send_message или /broadcast"
                                }
                            ],
                            "responses": {
                                "200": {"description": "Статус найден"},
                                "400": {"description": "Некорректный ID"},
                                "401": {"description": "Нет или неверный API-ключ"},
                                "404": {"description": "Сообщение не найдено"}
                            }
                        }
                    },
                    "/broadcast": {
                        "post": {
                            "summary": "Массовая рассылка",
                            "parameters": [
                                {
                                    "name": "body",
                                    "in": "body",
                                    "required": True,
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "recipients": {
                                                "type": "array",
                                                "items": {"type": "string"},
                                                "example": ["user1@arsenal", "user2@arsenal"]
                                            },
                                            "message": {
                                                "type": "string",
                                                "maxLength": 256,
                                                "example": "Всем привет!"
                                            },
                                            "priority": {
                                                "type": "string",
                                                "enum": ["low", "normal", "high", "critical"],
                                                "default": "normal"
                                            }
                                        }
                                    }
                                }
                            ],
                            "responses": {
                                "200": {"description": "Рассылка выполнена"}
                            }
                        }
                    }
                }
            })
            spec["paths"].update(_EXTRA_SWAGGER_PATHS)
            return jsonify(spec)
        
        allowed_methods = ['POST']
        if self.config.get('rest_api', {}).get('allow_get', False):
            allowed_methods.append('GET')

        @self._route(self.config['rest_api']['endpoint'], methods=allowed_methods)
        def send_message():
            """
            Отправить сообщение
            ---
            get:
              summary: Отправить сообщение (GET)
              parameters:
                - name: to
                  in: query
                  required: true
                  type: string
                - name: message
                  in: query
                  required: true
                  type: string
                - name: priority
                  in: query
                  type: string
                  enum: [low, normal, high, critical]
              responses:
                200:
                  description: Успешно
            post:
              summary: Отправить сообщение (POST)
              parameters:
                - name: body
                  in: body
                  required: true
                  schema:
                    type: object
                    properties:
                      to:
                        type: string
                      message:
                        type: string
                      priority:
                        type: string
                        enum: [low, normal, high, critical]
              responses:
                200:
                  description: Успешно
            """
            auth_error = self._check_api_key()
            if auth_error:
                return auth_error
            limited = self._rate_limited()
            if limited:
                return limited

            cfg = self._rest_cfg()
            try:
                if request.method == 'GET':
                    if not cfg.get('allow_get', False):
                        return self._err("method_not_allowed", "GET disabled. Use POST.", 405)
                    to_user = request.args.get('to')
                    message = request.args.get('message')
                    raw_priority = request.args.get('priority')
                    raw_type = raw_payload = None
                    raw_external_id = request.args.get('external_id')
                else:
                    data = request.get_json(silent=True)
                    if not data or not isinstance(data, dict):
                        return self._err("bad_request", "No JSON data provided", 400)

                    raw_priority = data.get('priority')
                    raw_type = data.get('type')
                    raw_payload = data.get('payload')
                    raw_external_id = data.get('external_id')
                    to_user = None
                    for field in _TO_FIELDS:
                        if field in data:
                            to_user = data[field]
                            break

                    message = None
                    for field in _TEXT_FIELDS:
                        if field in data:
                            message = str(data[field])
                            break

                    if not message and normalize_message_type_safe(raw_type) == "text":
                        if cfg.get('legacy_payload_as_message', False):
                            # устаревший режим: тело запроса целиком уходит как текст
                            payload_dump = {k: v for k, v in data.items() if k not in _SERVICE_FIELDS}
                            message = json.dumps(payload_dump, ensure_ascii=False)
                        else:
                            return self._err("bad_request", "Missing 'message' field", 400)

                if not to_user or (not message and normalize_message_type_safe(raw_type) == "text"):
                    return self._err("bad_request", "Missing 'to' or 'message' field", 400)

                try:
                    prepared = self._prepare(message, raw_priority, raw_type, raw_payload, raw_external_id)
                except ValueError as exc:
                    return self._err("bad_request", str(exc), 400)
                message, priority = prepared["message"], prepared["priority"]
                external_id = prepared["external_id"]

                existing = self._find_duplicate(external_id)
                if existing:
                    return jsonify(self._duplicate_body(existing)), 200

                msg_id = str(uuid.uuid4())
                recorded = self._record_queued(
                    msg_id, to_user, message, priority, prepared["type"], prepared["payload"], external_id
                )
                if recorded == "duplicate":
                    existing = self._find_duplicate(external_id)
                    if existing:
                        return jsonify(self._duplicate_body(existing)), 200

                def _extra(body):
                    body["type"] = prepared["type"]
                    if external_id:
                        body["external_id"] = external_id
                    if prepared["payload"]:
                        body["payload"] = prepared["payload"]
                    return body

                kafka_config = self.config.get("kafka", {})
                if kafka_config.get("enabled", False) and kafka_config.get("use_for_api_queue", False):
                    api_request = {
                        "to": to_user,
                        "message": message,
                        "message_id": msg_id,
                        "priority": priority,
                        "type": prepared["type"],
                        "payload": prepared["payload"],
                        "external_id": external_id,
                        "timestamp": datetime.now().isoformat(),
                        "api_key_used": bool(cfg.get("api_key"))
                    }
                    published = self._publish_to_kafka(
                        kafka_config.get("topics", {}).get("api_requests", "xmpp-api-requests"),
                        api_request,
                    )
                    if published:
                        self._log(f"API запрос отправлен в Kafka: {to_user}", "INFO")
                        return jsonify(_extra({
                            "status": "queued",
                            "message_id": msg_id,
                            "priority": priority,
                            "message_status": "queued",
                            "topic": kafka_config.get("topics", {}).get("api_requests", "xmpp-api-requests"),
                            "to": to_user,
                            "message": message,
                        })), 202
                    self._log("Не удалось отправить API запрос в Kafka, выполняем синхронную отправку", "WARNING")

                if not self.message_handler:
                    self._log("Message handler not configured", "ERROR")
                    self._mark_failed(msg_id)
                    return self._err("internal", "Message handler not configured", 500, message_id=msg_id)

                try:
                    send_time = datetime.now()
                    success = self._call_handler(to_user, prepared, msg_id)
                    if success and kafka_config.get("enabled", False) and kafka_config.get("publish_api_requests", True):
                        self._publish_message_event("SENT", to_user, message)
                except Exception as e:
                    self._log(f"Message handler crashed: {str(e)}", "ERROR")
                    self._mark_failed(msg_id)
                    return self._err("internal", f"Message handler error: {str(e)}", 500, message_id=msg_id)

                if success:
                    self._log(f"Сообщение отправлено {to_user}: {message[:50]}", "INFO")
                    return jsonify(_extra({
                        "status": "success",
                        "message_id": msg_id,
                        "priority": priority,
                        "message_status": "queued",
                        "to": to_user,
                        "message": message,
                        "send_time": send_time.isoformat()
                    })), 200
                else:
                    self._log(f"Ошибка отправки {to_user}", "ERROR")
                    self._mark_failed(msg_id)
                    return self._err("internal", "Failed to send message - XMPP client not connected or error",
                                     500, message_id=msg_id)

            except Exception as e:
                self._log(f"API error: {str(e)}", "ERROR")
                return self._err("internal", str(e), 500)

        @self._route('/health', methods=['GET'])
        def health_check():
            return jsonify({"status": "running"}), 200

        @self._route('/stats', methods=['GET'])
        def get_stats():
            auth_error = self._check_api_key()
            if auth_error:
                return auth_error
            try:
                db_path = MESSAGES_DB_PATH
                if not os.path.exists(db_path):
                    return jsonify({"status": "success", "stats": {"total_messages": 0, "sent": 0, "received": 0}}), 200
                
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM messages")
                total = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM messages WHERE type = 'SENT'")
                sent = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM messages WHERE type = 'RECEIVED'")
                received = cursor.fetchone()[0]
                conn.close()
                return jsonify({"status": "success", "stats": {"total_messages": total, "sent": sent, "received": received}}), 200
            except Exception as e:
                return self._err("internal", str(e), 500)
        
        @self._route('/get_messages', methods=['GET'])
        def get_messages():
            auth_error = self._check_api_key()
            if auth_error:
                return auth_error
            try:
                limit = request.args.get('limit', 100, type=int)
                try:
                    rows = self._get_logger().list_messages(
                        limit=limit,
                        msg_type=request.args.get('type'),
                        status=request.args.get('status'),
                        priority=request.args.get('priority'),
                    )
                except ValueError as exc:
                    return self._err("bad_request", str(exc), 400)
                messages = [
                    {
                        "message_id": r["message_id"], "type": r["type"],
                        "sender": r["sender"], "recipient": r["recipient"],
                        "message": r["message"], "priority": r["priority"],
                        "status": r["status"], "send_time": r["send_time"],
                        "delivery_time": r["delivery_time"], "read_time": r["read_time"],
                        "log_time": r["log_time"],
                    }
                    for r in rows
                ]
                return jsonify({"status": "success", "count": len(messages), "messages": messages}), 200
            except Exception as e:
                return self._err("internal", str(e), 500)
        
        @self._route('/messages/<message_id>', methods=['GET'])
        def get_message_status(message_id):
            """Точечный статус исходящего сообщения по ID."""
            auth_error = self._check_api_key()
            if auth_error:
                return auth_error
            if not _MESSAGE_ID_RE.match(message_id):
                return self._err("bad_request", "Invalid message id", 400)
            try:
                row = self._get_logger().get_message(message_id)
            except Exception as e:
                self._log(f"Ошибка чтения статуса {message_id}: {e}", "ERROR")
                return self._err("internal", str(e), 500)
            if not row:
                return self._err("not_found", "Message not found", 404, message_id=message_id)
            return jsonify({"status": "success", "message": {k: row.get(k) for k in _STATUS_FIELDS}}), 200

        @self._route('/messages/status', methods=['POST'])
        def messages_status():
            """Пакетный статус: POST {"ids": [message_id, ...]} (до 100). Для синхронизации из Oracle."""
            auth_error = self._check_api_key()
            if auth_error:
                return auth_error
            limited = self._rate_limited()
            if limited:
                return limited
            data = request.get_json(silent=True)
            ids = data.get('ids') if isinstance(data, dict) else None
            if not isinstance(ids, list) or not ids:
                return self._err("bad_request", "'ids' must be a non-empty list", 400)
            if len(ids) > MAX_STATUS_BATCH:
                return self._err("bad_request", f"'ids': не более {MAX_STATUS_BATCH}", 400)
            if any(not isinstance(i, str) or not _MESSAGE_ID_RE.match(i) for i in ids):
                return self._err("bad_request", "Invalid message id in 'ids'", 400)
            try:
                rows = self._get_logger().get_messages_by_ids(ids)
            except Exception as e:
                self._log(f"Ошибка пакетного статуса: {e}", "ERROR")
                return self._err("internal", str(e), 500)
            found = {r["message_id"]: r for r in rows}
            messages = [{k: found[i].get(k) for k in _STATUS_FIELDS} for i in dict.fromkeys(ids) if i in found]
            not_found = [i for i in dict.fromkeys(ids) if i not in found]
            return jsonify({"status": "success", "count": len(messages),
                            "messages": messages, "not_found": not_found}), 200

        @self._route('/broadcast', methods=['POST'])
        def broadcast():
            auth_error = self._check_api_key()
            if auth_error:
                return auth_error
            limited = self._rate_limited()
            if limited:
                return limited
            try:
                cfg = self._rest_cfg()
                data = request.get_json(silent=True) or {}
                recipients = data.get('recipients', [])
                message = data.get('message', '')
                raw_type = data.get('type')

                if not recipients or (not message and normalize_message_type_safe(raw_type) == "text"):
                    return self._err("bad_request", "Missing 'recipients' or 'message' field", 400)
                if not isinstance(recipients, list):
                    return self._err("bad_request", "'recipients' must be a list", 400)
                max_recipients = int(cfg.get('broadcast_max_recipients', DEFAULT_BROADCAST_MAX_RECIPIENTS))
                if len(recipients) > max_recipients:
                    return self._err("bad_request", f"'recipients': не более {max_recipients}", 400)
                if any(not isinstance(r, str) or not r.strip() for r in recipients):
                    return self._err("bad_request", "'recipients' must contain non-empty strings", 400)

                try:
                    prepared = self._prepare(
                        message, data.get('priority'), raw_type, data.get('payload'), data.get('external_id')
                    )
                except ValueError as exc:
                    return self._err("bad_request", str(exc), 400)
                message, priority = prepared["message"], prepared["priority"]

                if not self.message_handler:
                    return self._err("internal", "Message handler not configured", 500)

                results = []
                success_count = 0
                duplicates = 0

                for to_user in recipients:
                    # external_id рассылки уникализируется получателем: повтор запроса не шлёт дубли
                    ext = f"{prepared['external_id']}:{to_user}" if prepared["external_id"] else None
                    existing = self._find_duplicate(ext)
                    if existing:
                        duplicates += 1
                        success_count += 1
                        results.append({"to": to_user, "message_id": existing["message_id"],
                                        "status": "success", "duplicate": True})
                        continue
                    msg_id = str(uuid.uuid4())
                    if self._record_queued(msg_id, to_user, message, priority,
                                           prepared["type"], prepared["payload"], ext) == "duplicate":
                        existing = self._find_duplicate(ext)
                        if existing:
                            duplicates += 1
                            success_count += 1
                            results.append({"to": to_user, "message_id": existing["message_id"],
                                            "status": "success", "duplicate": True})
                            continue
                    try:
                        success = self._call_handler(to_user, prepared, msg_id)
                        if not success:
                            self._mark_failed(msg_id)
                        results.append({"to": to_user, "message_id": msg_id,
                                        "status": "success" if success else "failed"})
                        if success:
                            success_count += 1
                    except Exception as e:
                        self._mark_failed(msg_id)
                        results.append({"to": to_user, "message_id": msg_id, "status": f"error: {str(e)}"})

                return jsonify({
                    "status": "broadcast complete",
                    "priority": priority,
                    "type": prepared["type"],
                    "total": len(recipients),
                    "success": success_count,
                    "failed": len(recipients) - success_count,
                    "duplicates": duplicates,
                    "results": results
                }), 200
            except Exception as e:
                return self._err("internal", str(e), 500)

        # ------------------------------------------------ эндпоинты для ТСД (ТЗ-1, пп. 11–13)
        def _device_request():
            """Общая проверка для запросов терминала: лимит частоты, deviceId, токен устройства.
            Возвращает (device_id, None) или (None, ответ-ошибка)."""
            limited = self._rate_limited()
            if limited:
                return None, limited
            raw = request.args.get('deviceId')
            if raw is None:
                body = request.get_json(silent=True)
                raw = body.get('deviceId') if isinstance(body, dict) else None
            try:
                device_id = validate_device_id(raw)
            except ValueError as exc:
                return None, self._err("bad_request", str(exc), 400)
            if not self._registry().verify_token(device_id, request.headers.get('X-Device-Token')):
                return None, self._err("unauthorized", "Invalid device token", 401)
            return device_id, None

        @self.app.route('/v1/auth/xmpp', methods=['GET'])
        def auth_xmpp():
            device_id, error = _device_request()
            if error:
                return error
            creds = self._registry().credentials(device_id)
            if not creds:
                self._log(f"Учётные данные не выданы: терминал {device_id} не зарегистрирован", "WARNING")
                return self._err("not_found", "Device is not registered or has no XMPP account", 404)
            cfg = self._rest_cfg()
            xmpp_cfg = self.config.get("xmpp", {})
            self._log(f"Учётные данные XMPP выданы терминалу {device_id}", "INFO")
            return jsonify({
                "xmppHost": xmpp_cfg.get("server", ""),
                "xmppPort": int(xmpp_cfg.get("port", 5222)),
                "xmppLogin": creds[0],
                "xmppPassword": creds[1],
                "apiHost": cfg.get("public_host") or request.host.split(":")[0],
                "apiPort": int(cfg.get("public_port") or cfg.get("port", 80)),
            }), 200

        @self.app.route('/v1/wakeup/confirm', methods=['POST'])
        def wakeup_confirm():
            device_id, error = _device_request()
            if error:
                return error
            raw_task = request.args.get('taskId')
            if raw_task is None:
                body = request.get_json(silent=True)
                raw_task = body.get('taskId') if isinstance(body, dict) else None
            task_id = str(raw_task or "").strip()
            if not task_id or len(task_id) > 64 or not task_id.isprintable():
                return self._err("bad_request", "taskId: от 1 до 64 печатных символов", 400)
            self._registry().record_confirm(device_id, task_id)
            self._log(f"Подтверждение пробуждения: терминал {device_id}, задание {task_id}", "INFO")
            return jsonify({"status": "ok"}), 200

        @self.app.route('/v1/config/trusted-senders', methods=['GET'])
        def trusted_senders():
            device_id, error = _device_request()
            if error:
                return error
            senders = [str(x) for x in self._rest_cfg().get("trusted_senders", []) if str(x).strip()]
            return jsonify({"senders": senders}), 200

        # ------------------------------------------------ административный API реестра терминалов
        @self.app.route('/v1/admin/devices', methods=['GET'])
        def admin_list_devices():
            auth_error = self._require_admin()
            if auth_error:
                return auth_error
            return jsonify({"status": "success", "devices": self._registry().list()}), 200

        @self.app.route('/v1/admin/devices/<device_id>', methods=['GET', 'PUT', 'DELETE'])
        def admin_device(device_id):
            auth_error = self._require_admin()
            if auth_error:
                return auth_error
            try:
                device_id = validate_device_id(device_id)
            except ValueError as exc:
                return self._err("bad_request", str(exc), 400)
            reg = self._registry()
            if request.method == 'GET':
                record = reg.get(device_id)
                if not record:
                    return self._err("not_found", "Device not found", 404)
                return jsonify({"status": "success", "device": record}), 200
            if request.method == 'DELETE':
                if not reg.delete(device_id):
                    return self._err("not_found", "Device not found", 404)
                self._log(f"Терминал {device_id} удалён из реестра", "INFO")
                return jsonify({"status": "success", "deleted": device_id}), 200
            data = request.get_json(silent=True)
            if not isinstance(data, dict):
                return self._err("bad_request", "No JSON data provided", 400)
            try:
                record, new_token = reg.upsert(
                    device_id,
                    xmpp_login=data.get("xmpp_login"),
                    xmpp_password=data.get("xmpp_password"),
                    active=data.get("active"),
                    token=data.get("token"),
                    generate_token=bool(data.get("generate_token", False)),
                )
            except ValueError as exc:
                return self._err("bad_request", str(exc), 400)
            self._log(f"Терминал {device_id} обновлён в реестре", "INFO")
            body = {"status": "success", "device": record}
            if new_token:
                body["token"] = new_token  # показывается один раз
            return jsonify(body), 200

    def start(self):
        # Add security headers middleware
        @self.app.after_request
        def set_security_headers(response):
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['X-Frame-Options'] = 'DENY'
            response.headers['X-XSS-Protection'] = '1; mode=block'
            response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
            return response
        
        try:
            host = self.config['rest_api']['host']
            port = self.config['rest_api']['port']
            api_key = self.config.get('rest_api', {}).get('api_key', '')
            if host in ('0.0.0.0', '::') and not api_key:
                self._log(
                    "ВНИМАНИЕ: REST слушает все интерфейсы без api_key. "
                    "Задайте rest_api.api_key в config.json",
                    "WARNING",
                )
            self.server_thread = Thread(target=self._run_server, args=(host, port), daemon=True)
            self.server_thread.start()
            self.is_running = True
            self._log(f"REST API server started on {host}:{port}", "INFO")
            self._setup_kafka_consumers()
            if getattr(self, '_swagger_enabled', False):
                self._log(f"Swagger UI: http://127.0.0.1:{port}/apidocs", "INFO")
            if api_key:
                self._log("REST API: авторизация по заголовку X-API-Key включена", "INFO")
        except Exception as e:
            self._log(f"Failed to start REST server: {str(e)}", "ERROR")
    
    def _run_server(self, host, port):
        try:
            from waitress import serve
            serve(self.app, host=host, port=port, threads=8)
        except ImportError:
            self._log("Waitress не установлен, используется встроенный сервер Flask", "WARNING")
            self.app.run(host=host, port=port, threaded=True, debug=False)
    
    def stop(self):
        self.is_running = False
        if self.kafka_broker:
            self.kafka_broker.disconnect()
            self._log("Kafka брокер отключен", "INFO")
        self._log("REST API server stopped", "INFO")
