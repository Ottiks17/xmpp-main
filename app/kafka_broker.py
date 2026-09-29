"""Kafka broker для публикации сообщений и очередирования запросов."""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Optional
from threading import Thread, Event

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaBroker:
    """Управление публикацией и потреблением сообщений Kafka."""

    def __init__(
        self,
        bootstrap_servers: str | list[str],
        log_callback: Optional[Callable[[str, str], None]] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.log_callback = log_callback
        self.producer: Optional[KafkaProducer] = None
        self.consumers: dict[str, KafkaConsumer] = {}
        self.consumer_threads: dict[str, Thread] = {}
        self.stop_consumers = Event()

    def _log(self, message: str, level: str = "INFO"):
        """Логирование с callback."""
        if self.log_callback:
            self.log_callback(f"[Kafka] {message}", level)
        else:
            logger.log(getattr(logging, level), message)

    def connect(self) -> bool:
        """Подключение к Kafka и инициализация producer."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                    "utf-8"
                ),
                acks="all",  # Гарантия доставки
                retries=3,
                request_timeout_ms=10000,
            )
            self._log("Успешно подключено к Kafka", "INFO")
            return True
        except Exception as exc:
            self._log(f"Ошибка подключения к Kafka: {exc}", "ERROR")
            return False

    def publish(
        self, topic: str, message: dict[str, Any], key: Optional[str] = None
    ) -> bool:
        """
        Публикация сообщения в топик.

        Args:
            topic: Название топика
            message: Сообщение (будет сохранено как JSON)
            key: Ключ сообщения (опционально)

        Returns:
            True если успешно, False иначе
        """
        if not self.producer:
            self._log("Producer не инициализирован", "ERROR")
            return False

        try:
            future = self.producer.send(
                topic, value=message, key=key.encode() if key else None
            )
            # Блокирующая отправка с timeout
            future.get(timeout=5)
            self._log(f"Опубликовано сообщение в {topic}", "DEBUG")
            return True
        except KafkaError as exc:
            self._log(f"Ошибка публикации в {topic}: {exc}", "ERROR")
            return False
        except Exception as exc:
            self._log(f"Неожиданная ошибка при публикации: {exc}", "ERROR")
            return False

    def subscribe(
        self,
        topics: list[str],
        consumer_group: str,
        handler: Callable[[str, dict[str, Any]], None],
        auto_offset_reset: str = "latest",
    ) -> bool:
        """
        Подписка на топики с обработчиком сообщений.

        Args:
            topics: Список топиков для подписки
            consumer_group: Группа потребителей
            handler: Функция-обработчик (topic, message) -> None
            auto_offset_reset: 'earliest' или 'latest'

        Returns:
            True если успешно, False иначе
        """
        try:
            consumer_key = f"{consumer_group}:{','.join(topics)}"

            if consumer_key in self.consumers:
                self._log(f"Уже подписаны на {topics}", "WARNING")
                return False

            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=consumer_group,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                session_timeout_ms=6000,
                request_timeout_ms=10000,
            )

            self.consumers[consumer_key] = consumer

            # Запуск потока для обработки сообщений
            thread = Thread(
                target=self._consume_messages,
                args=(consumer_key, consumer, handler),
                daemon=True,
            )
            thread.start()
            self.consumer_threads[consumer_key] = thread

            self._log(f"Подписаны на {topics} в группе {consumer_group}", "INFO")
            return True

        except Exception as exc:
            self._log(f"Ошибка подписки: {exc}", "ERROR")
            return False

    def _consume_messages(
        self, consumer_key: str, consumer: KafkaConsumer, handler: Callable
    ):
        """Поток для потребления сообщений с retry logic."""
        import time
        try:
            while not self.stop_consumers.is_set():
                for message in consumer:
                    if self.stop_consumers.is_set():
                        break
                    
                    # Retry logic: до 3 попыток обработки
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            handler(message.topic, message.value)
                            break  # Success, exit retry loop
                        except Exception as exc:
                            if attempt < max_retries - 1:
                                self._log(
                                    f"Ошибка обработки сообщения (попытка {attempt + 1}/{max_retries}): {exc}",
                                    "WARNING",
                                )
                                # Exponential backoff
                                time.sleep(2 ** attempt)
                            else:
                                self._log(
                                    f"Исчерпаны попытки обработки сообщения из {message.topic}: {exc}",
                                    "ERROR",
                                )
        except Exception as exc:
            self._log(f"Ошибка в consumer потоке {consumer_key}: {exc}", "ERROR")
        finally:
            consumer.close()
            self.consumers.pop(consumer_key, None)

    def unsubscribe(self, topics: list[str], consumer_group: str) -> bool:
        """Отписка от топиков."""
        consumer_key = f"{consumer_group}:{','.join(topics)}"
        if consumer_key in self.consumers:
            self.consumers[consumer_key].close()
            self.consumers.pop(consumer_key, None)
            return True
        return False

    def disconnect(self):
        """Отключение от Kafka."""
        self.stop_consumers.set()

        # Закрыть все consumers
        for consumer in self.consumers.values():
            consumer.close()
        self.consumers.clear()

        # Дождаться завершения потоков
        for thread in self.consumer_threads.values():
            thread.join(timeout=2)
        self.consumer_threads.clear()

        # Закрыть producer
        if self.producer:
            self.producer.close()
            self.producer = None

        self._log("Отключено от Kafka", "INFO")

    def __del__(self):
        """Очистка при удалении объекта."""
        try:
            self.disconnect()
        except Exception:
            pass

