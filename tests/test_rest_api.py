"""Тесты REST API — против реального rest_server.RESTServer.

Раньше этот файл содержал самодостаточную копию сервера и не проверял
продакшен-код. Теперь тесты строят реальный RESTServer и ходят по нему через
Flask test_client, покрывая /send_message, /broadcast, /health, /stats,
авторизацию по X-API-Key и ограничение частоты запросов.
"""
import json
import os
import tempfile

import rest_server
from app.storage import MessageLogger
from rest_server import RESTServer


_NO_HANDLER = object()


def _make_server(tmp, api_key="", message_handler=_NO_HANDLER, allow_get=False,
                 **rest_overrides):
    """Собирает RESTServer с временной БД и возвращает (server, client, logger)."""
    logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
    if message_handler is _NO_HANDLER:
        message_handler = lambda *a, **kw: True
    config = {
        "rest_api": {
            "host": "127.0.0.1",
            "port": 8080,
            "endpoint": "/send_message",
            "api_key": api_key,
            "allow_get": allow_get,
            **rest_overrides,
        }
    }
    server = RESTServer(config, message_handler=message_handler, message_logger=logger)
    return server, server.app.test_client(), logger


def _post(client, headers=None, **body):
    return client.post("/send_message", json=body, headers=headers or {})


# ---------------------------------------------------------------------- #
#  /send_message
# ---------------------------------------------------------------------- #

def test_send_message_success():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp)
        r = _post(client, to="user@arsenal", message="hello")
        assert r.status_code == 200
        data = r.get_json()
        assert data["status"] == "success"
        assert data["message_id"]
        assert data["priority"] == "normal"
        assert data["to"] == "user@arsenal"


def test_send_message_missing_to_returns_400():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp)
        assert _post(client, message="hello").status_code == 400


def test_send_message_empty_body_returns_400():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp)
        assert client.post("/send_message").status_code == 400  # нет JSON-тела


def test_send_message_too_long_returns_400():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp)
        r = _post(client, to="user@arsenal", message="x" * 257)
        assert r.status_code == 400


def test_send_message_bad_priority_returns_400():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp)
        r = _post(client, to="user@arsenal", message="hi", priority="urgent")
        assert r.status_code == 400


def test_send_message_payload_without_message_field_is_sent_as_json():
    """Отсутствие поля message не даёт 400: остальной payload уходит JSON-строкой."""
    calls = []

    def handler(to_user, message, priority, msg_id):
        calls.append((to_user, message, priority, msg_id))
        return True

    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp, message_handler=handler)
        r = _post(client, to="user@arsenal", custom="payload")
        assert r.status_code == 200
        assert json.loads(calls[0][1]) == {"to": "user@arsenal", "custom": "payload"}


def test_send_message_handler_not_configured_returns_500():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp, message_handler=None)
        r = _post(client, to="user@arsenal", message="hi")
        assert r.status_code == 500


def test_send_message_handler_returns_false_returns_500():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp, message_handler=lambda *a: False)
        assert _post(client, to="user@arsenal", message="hi").status_code == 500


def test_send_message_handler_raises_returns_500():
    def handler(*a):
        raise RuntimeError("boom")

    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp, message_handler=handler)
        assert _post(client, to="user@arsenal", message="hi").status_code == 500


# ---------------------------------------------------------------------- #
#  Авторизация по X-API-Key
# ---------------------------------------------------------------------- #

def test_api_key_required():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp, api_key="secret")
        assert _post(client, to="user@arsenal", message="hi").status_code == 401
        assert _post(client, to="user@arsenal", message="hi",
                     headers={"X-API-Key": "wrong"}).status_code == 401
        assert _post(client, to="user@arsenal", message="hi",
                     headers={"X-API-Key": "secret"}).status_code == 200


def test_api_key_via_query_param():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp, api_key="secret")
        r = client.post("/send_message?api_key=secret",
                        json={"to": "user@arsenal", "message": "hi"})
        assert r.status_code == 200


def test_no_api_key_configured_skips_auth():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp)
        assert _post(client, to="user@arsenal", message="hi").status_code == 200


# ---------------------------------------------------------------------- #
#  GET-режим и rate limiting
# ---------------------------------------------------------------------- #

def test_get_disabled_by_default():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp)
        assert client.get("/send_message?to=a@arsenal&message=hi").status_code == 405


def test_get_allowed_when_configured():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp, allow_get=True)
        r = client.get("/send_message?to=a@arsenal&message=hi")
        assert r.status_code == 200


def test_rate_limiter_unit():
    rl = rest_server.RateLimiter(max_requests=2, window_seconds=60)
    assert rl.is_allowed("1.1.1.1") is True
    assert rl.is_allowed("1.1.1.1") is True
    assert rl.is_allowed("1.1.1.1") is False
    assert rl.is_allowed("2.2.2.2") is True  # другой IP не ограничен


def test_rate_limiter_expires_old_requests(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(rest_server, "time", lambda: now[0])
    rl = rest_server.RateLimiter(max_requests=1, window_seconds=60)
    assert rl.is_allowed("1.1.1.1") is True
    assert rl.is_allowed("1.1.1.1") is False
    now[0] = 1100.0  # за пределами окна
    assert rl.is_allowed("1.1.1.1") is True


def test_send_message_rate_limited_returns_429():
    with tempfile.TemporaryDirectory() as tmp:
        server, client, _ = _make_server(tmp)
        server.rate_limiter.max_requests = 2
        assert _post(client, to="a@arsenal", message="1").status_code == 200
        assert _post(client, to="a@arsenal", message="2").status_code == 200
        assert _post(client, to="a@arsenal", message="3").status_code == 429


# ---------------------------------------------------------------------- #
#  /health, /stats, /broadcast
# ---------------------------------------------------------------------- #

def test_health_no_auth():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp, api_key="secret")
        r = client.get("/health")
        assert r.status_code == 200
        assert r.get_json() == {"status": "running"}


def test_stats_counts(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp)
        _post(client, to="user@arsenal", message="hi")
        monkeypatch.setattr(rest_server, "MESSAGES_DB_PATH", os.path.join(tmp, "t.db"))
        r = client.get("/stats")
        assert r.status_code == 200
        stats = r.get_json()["stats"]
        assert stats["total_messages"] >= 1


def test_broadcast_success():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp)
        r = client.post("/broadcast", json={
            "recipients": ["a@arsenal", "b@arsenal"],
            "message": "hi",
            "priority": "high",
        })
        assert r.status_code == 200
        data = r.get_json()
        assert data["total"] == 2 and data["success"] == 2 and data["failed"] == 0
        ids = [item["message_id"] for item in data["results"]]
        assert len(set(ids)) == 2  # у каждого получателя свой id


def test_broadcast_missing_fields_returns_400():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _ = _make_server(tmp)
        assert client.post("/broadcast", json={"recipients": []}).status_code == 400
        assert client.post("/broadcast", json={"message": "hi"}).status_code == 400
        assert client.post("/broadcast", json={
            "recipients": "a@arsenal", "message": "hi",
        }).status_code == 400  # recipients не список
