
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
from app.validation import normalize_priority, validate_message
from app.kafka_broker import KafkaBroker
from app.storage import MessageLogger

# ID сообщения: uuid4 или старые числовые ID
_MESSAGE_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
# Поля, которые отдаёт GET /messages/{id}
_STATUS_FIELDS = ("message_id", "recipient", "priority", "status", "send_time", "delivery_time", "read_time")


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

        # Инициализация Kafka (если включена и не передан внешний брокер)
        if not self.kafka_broker and self.config.get("kafka", {}).get("enabled", False):
            self._init_kafka()

        # Настройка Swagger
        self._setup_swagger()
        self._setup_routes()
        
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)
    
    def _check_api_key(self):
        api_key = self.config.get("rest_api", {}).get("api_key", "")
        if not api_key:
            return None
        provided = request.headers.get("X-API-Key") or request.args.get("api_key", "")
        if not hmac.compare_digest(provided, api_key):
            return jsonify({"error": "Unauthorized. Provide X-API-Key header or api_key query param."}), 401
        return None

    def _get_logger(self):
        """MessageLogger для чтения. Если не передан в конструктор, создаётся по пути из констант."""
        if self.message_logger is None:
            self.message_logger = MessageLogger(db_path=MESSAGES_DB_PATH)
        return self.message_logger

    def _record_queued(self, msg_id, to_user, message, priority):
        """Синхронно создаёт строку queued, чтобы GET /messages/{id} видел сообщение сразу."""
        if not self.message_logger:
            return
        try:
            self.message_logger.log_message(
                "SENT", "api", to_user, message,
                message_id=msg_id, priority=priority, status="queued",
            )
        except Exception as exc:
            self._log(f"Не удалось записать queued {msg_id}: {exc}", "WARNING")

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
            msg_id = message.get("message_id")
            if not msg_id:
                # запись положил внешний продюсер напрямую в топик
                msg_id = str(uuid.uuid4())
                self._record_queued(msg_id, to_user, text, priority)

            self._log(f"Обработка Kafka API-запроса для {to_user}", "INFO")
            success = self.message_handler(to_user, text, priority, msg_id)
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
            return jsonify({
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
        
        allowed_methods = ['POST']
        if self.config.get('rest_api', {}).get('allow_get', False):
            allowed_methods.append('GET')

        @self.app.route(self.config['rest_api']['endpoint'], methods=allowed_methods)
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
            
            # Rate limiting
            client_ip = request.remote_addr
            if not self.rate_limiter.is_allowed(client_ip):
                self._log(f"Rate limit exceeded for {client_ip}", "WARNING")
                return jsonify({"error": "Too many requests. Rate limit exceeded."}), 429

            try:
                if request.method == 'GET':
                    if not self.config.get('rest_api', {}).get('allow_get', False):
                        return jsonify({"error": "GET disabled. Use POST."}), 405
                    to_user = request.args.get('to')
                    message = request.args.get('message')
                    raw_priority = request.args.get('priority')
                else:
                    data = request.get_json()
                    if not data:
                        return jsonify({"error": "No JSON data provided"}), 400
                    
                    raw_priority = data.get('priority')
                    to_user = None
                    for field in ['to', 'recipient', 'user', 'username', 'jid', 'send_to', 'target']:
                        if field in data:
                            to_user = data[field]
                            break
                    
                    message = None
                    for field in ['message', 'msg', 'text', 'body', 'content']:
                        if field in data:
                            message = str(data[field])
                            break
                    
                    if not message:
                        # priority — служебное поле, в текст сообщения не попадает
                        payload = {k: v for k, v in data.items() if k != 'priority'}
                        message = json.dumps(payload, ensure_ascii=False)
                
                if not to_user or not message:
                    return jsonify({"error": "Missing 'to' or 'message' field"}), 400

                try:
                    message = validate_message(str(message))
                    priority = normalize_priority(raw_priority)
                except ValueError as exc:
                    return jsonify({"error": str(exc)}), 400

                msg_id = str(uuid.uuid4())
                self._record_queued(msg_id, to_user, message, priority)
                
                kafka_config = self.config.get("kafka", {})
                if kafka_config.get("enabled", False) and kafka_config.get("use_for_api_queue", False):
                    api_request = {
                        "to": to_user,
                        "message": message,
                        "message_id": msg_id,
                        "priority": priority,
                        "timestamp": datetime.now().isoformat(),
                        "api_key_used": bool(self.config.get("rest_api", {}).get("api_key"))
                    }
                    published = self._publish_to_kafka(
                        kafka_config.get("topics", {}).get("api_requests", "xmpp-api-requests"),
                        api_request,
                    )
                    if published:
                        self._log(f"API запрос отправлен в Kafka: {to_user}", "INFO")
                        return jsonify({
                            "status": "queued",
                            "message_id": msg_id,
                            "priority": priority,
                            "message_status": "queued",
                            "topic": kafka_config.get("topics", {}).get("api_requests", "xmpp-api-requests"),
                            "to": to_user,
                            "message": message,
                        }), 202
                    self._log("Не удалось отправить API запрос в Kafka, выполняем синхронную отправку", "WARNING")

                if not self.message_handler:
                    self._log("Message handler not configured", "ERROR")
                    self._mark_failed(msg_id)
                    return jsonify({"error": "Message handler not configured", "message_id": msg_id}), 500

                try:
                    send_time = datetime.now()
                    success = self.message_handler(to_user, message, priority, msg_id)
                    if success and kafka_config.get("enabled", False) and kafka_config.get("publish_api_requests", True):
                        self._publish_message_event("SENT", to_user, message)
                except Exception as e:
                    self._log(f"Message handler crashed: {str(e)}", "ERROR")
                    self._mark_failed(msg_id)
                    return jsonify({"error": f"Message handler error: {str(e)}", "message_id": msg_id}), 500
                
                if success:
                    self._log(f"Сообщение отправлено {to_user}: {message[:50]}", "INFO")
                    return jsonify({
                        "status": "success",
                        "message_id": msg_id,
                        "priority": priority,
                        "message_status": "queued",
                        "to": to_user,
                        "message": message,
                        "send_time": send_time.isoformat()
                    }), 200
                else:
                    self._log(f"Ошибка отправки {to_user}", "ERROR")
                    self._mark_failed(msg_id)
                    return jsonify({"error": "Failed to send message - XMPP client not connected or error", "message_id": msg_id}), 500
                
            except Exception as e:
                self._log(f"API error: {str(e)}", "ERROR")
                return jsonify({"error": str(e)}), 500

        @self.app.route('/health', methods=['GET'])
        def health_check():
            return jsonify({"status": "running"}), 200

        @self.app.route('/stats', methods=['GET'])
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
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/get_messages', methods=['GET'])
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
                    return jsonify({"error": str(exc)}), 400
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
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/messages/<message_id>', methods=['GET'])
        def get_message_status(message_id):
            """Точечный статус исходящего сообщения по ID."""
            auth_error = self._check_api_key()
            if auth_error:
                return auth_error
            if not _MESSAGE_ID_RE.match(message_id):
                return jsonify({"error": "Invalid message id"}), 400
            try:
                row = self._get_logger().get_message(message_id)
            except Exception as e:
                self._log(f"Ошибка чтения статуса {message_id}: {e}", "ERROR")
                return jsonify({"error": str(e)}), 500
            if not row:
                return jsonify({"error": "Message not found", "message_id": message_id}), 404
            return jsonify({"status": "success", "message": {k: row.get(k) for k in _STATUS_FIELDS}}), 200

        @self.app.route('/broadcast', methods=['POST'])
        def broadcast():
            auth_error = self._check_api_key()
            if auth_error:
                return auth_error
            try:
                data = request.get_json(silent=True) or {}
                recipients = data.get('recipients', [])
                message = data.get('message', '')
                
                if not recipients or not message:
                    return jsonify({"error": "Missing 'recipients' or 'message' field"}), 400
                if not isinstance(recipients, list):
                    return jsonify({"error": "'recipients' must be a list"}), 400
                
                try:
                    message = validate_message(str(message))
                    priority = normalize_priority(data.get('priority'))
                except ValueError as exc:
                    return jsonify({"error": str(exc)}), 400

                if not self.message_handler:
                    return jsonify({"error": "Message handler not configured"}), 500

                results = []
                success_count = 0
                
                for to_user in recipients:
                    msg_id = str(uuid.uuid4())
                    self._record_queued(msg_id, to_user, message, priority)
                    try:
                        success = self.message_handler(to_user, message, priority, msg_id)
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
                    "total": len(recipients),
                    "success": success_count,
                    "failed": len(recipients) - success_count,
                    "results": results
                }), 200
            except Exception as e:
                return jsonify({"error": str(e)}), 500
    
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


