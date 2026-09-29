"""Этап 4: GET /messages/{id}, новые поля и фильтры в /get_messages."""
import os
import tempfile

from app.storage import MessageLogger
from rest_server import RESTServer


def _setup(tmp, api_key=""):
    logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
    config = {"rest_api": {"host": "127.0.0.1", "port": 8080, "endpoint": "/send_message",
                           "api_key": api_key, "allow_get": False}}
    server = RESTServer(config, message_handler=lambda *a: True, message_logger=logger)
    return server.app.test_client(), logger


def _send(client, headers=None, **extra):
    body = {"to": "user@arsenal", "message": "hi", **extra}
    r = client.post("/send_message", json=body, headers=headers or {})
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()["message_id"]


def test_status_lifecycle_queued_to_read():
    with tempfile.TemporaryDirectory() as tmp:
        client, logger = _setup(tmp)
        mid = _send(client, priority="high")

        m = client.get(f"/messages/{mid}").get_json()["message"]
        assert m["status"] == "queued" and m["priority"] == "high"
        assert m["message_id"] == mid and m["delivery_time"] is None

        logger.log_message("SENT", "srv@d/res", "user@arsenal", "hi", message_id=mid, priority="high")
        assert client.get(f"/messages/{mid}").get_json()["message"]["status"] == "sent"

        logger.mark_delivered(mid)
        m = client.get(f"/messages/{mid}").get_json()["message"]
        assert m["status"] == "delivered" and m["delivery_time"]

        logger.mark_read(mid)
        m = client.get(f"/messages/{mid}").get_json()["message"]
        assert m["status"] == "read" and m["read_time"]


def test_status_response_has_only_contract_fields():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _setup(tmp)
        mid = _send(client)
        m = client.get(f"/messages/{mid}").get_json()["message"]
        assert set(m) == {"message_id", "recipient", "priority", "status",
                          "send_time", "delivery_time", "read_time"}


def test_failed_status_visible():
    with tempfile.TemporaryDirectory() as tmp:
        client, logger = _setup(tmp)
        mid = _send(client)
        logger.mark_failed(mid)
        assert client.get(f"/messages/{mid}").get_json()["message"]["status"] == "failed"


def test_unknown_id_is_404():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _setup(tmp)
        r = client.get("/messages/does-not-exist")
        assert r.status_code == 404 and r.get_json()["message_id"] == "does-not-exist"


def test_bad_id_is_400():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _setup(tmp)
        assert client.get("/messages/" + "a" * 65).status_code == 400
        assert client.get("/messages/a%20b").status_code == 400


def test_status_requires_api_key():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _setup(tmp, api_key="secret")
        mid = _send(client, headers={"X-API-Key": "secret"})
        assert client.get(f"/messages/{mid}").status_code == 401
        r = client.get(f"/messages/{mid}", headers={"X-API-Key": "secret"})
        assert r.status_code == 200


def test_incoming_message_id_not_exposed_as_status():
    with tempfile.TemporaryDirectory() as tmp:
        client, logger = _setup(tmp)
        logger.log_message("RECEIVED", "u@d", "srv@d", "hello", message_id="in-1")
        assert client.get("/messages/in-1").status_code == 404


def test_get_messages_new_fields_and_filters():
    with tempfile.TemporaryDirectory() as tmp:
        client, logger = _setup(tmp)
        a = _send(client, priority="low")
        b = _send(client, priority="critical")
        logger.mark_delivered(b)

        body = client.get("/get_messages").get_json()
        assert body["count"] == 2
        first = body["messages"][0]
        assert first["message_id"] == b and first["priority"] == "critical" and first["status"] == "delivered"
        assert {"message_id", "priority", "status"} <= set(first)

        only_low = client.get("/get_messages?priority=low").get_json()["messages"]
        assert [m["message_id"] for m in only_low] == [a]
        only_delivered = client.get("/get_messages?status=delivered").get_json()["messages"]
        assert [m["message_id"] for m in only_delivered] == [b]


def test_get_messages_limit_is_clamped_and_bad_priority_400():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _setup(tmp)
        for _i in range(3):
            _send(client)
        assert client.get("/get_messages?limit=-1").get_json()["count"] == 1  # не «без лимита»
        assert client.get("/get_messages?limit=2").get_json()["count"] == 2
        assert client.get("/get_messages?priority=urgent").status_code == 400

