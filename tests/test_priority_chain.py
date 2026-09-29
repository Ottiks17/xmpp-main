"""Этап 3: priority и message_id по цепочке REST -> handler -> native -> XML."""
import os
import re
import tempfile
import uuid

from app.storage import MessageLogger
from app.xmpp.native import SimpleXMPPClient
from rest_server import RESTServer


def _config():
    return {"rest_api": {"host": "127.0.0.1", "port": 8080, "endpoint": "/send_message",
                         "api_key": "", "allow_get": True}}


def _server(handler, logger=None):
    calls = []

    def wrapped(*args):
        calls.append(args)
        return handler(*args)

    server = RESTServer(_config(), message_handler=wrapped, message_logger=logger)
    return server.app.test_client(), calls


# ---------------------------------------------------------------- native
def _client():
    c = SimpleXMPPClient("a@b", "pw", "srv")
    c.is_connected = True
    sent = []
    c._send_raw = sent.append
    return c, sent


def test_native_xml_has_priority_and_uuid():
    c, sent = _client()
    msg_id = c.send_message("u@d", "hi", priority="critical")
    uuid.UUID(msg_id)  # это валидный UUID
    xml = sent[0]
    assert f'id="{msg_id}"' in xml
    assert '<priority xmlns="urn:wakeup:msg:0" level="critical"/>' in xml
    assert "urn:xmpp:receipts" in xml and "urn:xmpp:chat-markers:0" in xml


def test_native_default_priority_is_normal():
    c, sent = _client()
    c.send_message("u@d", "hi")
    assert 'level="normal"' in sent[0]


def test_native_unknown_priority_not_sent():
    c, sent = _client()
    assert c.send_message("u@d", "hi", priority="urgent") is None
    assert sent == []


def test_queue_message_returns_id_and_keeps_priority():
    c, _ = _client()
    msg_id = c.queue_message("u@d", "hi", priority="high")
    uuid.UUID(msg_id)
    assert c.message_queue.get_nowait() == ("u@d", "hi", msg_id, "high")
    given = c.queue_message("u@d", "x", msg_id="fixed-1")
    assert given == "fixed-1"


# ---------------------------------------------------------------- REST
def test_rest_passes_priority_and_id_to_handler():
    with tempfile.TemporaryDirectory() as tmp:
        logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
        client, calls = _server(lambda *a: True, logger)
        r = client.post("/send_message", json={"to": "u@d", "message": "hi", "priority": "Critical"})
        assert r.status_code == 200
        body = r.get_json()
        assert body["priority"] == "critical" and body["message_status"] == "queued"
        uuid.UUID(body["message_id"])
        assert calls == [("u@d", "hi", "critical", body["message_id"])]
        row = logger.get_message(body["message_id"])
        assert row["status"] == "queued" and row["priority"] == "critical"


def test_rest_default_priority_normal():
    client, calls = _server(lambda *a: True)
    r = client.post("/send_message", json={"to": "u@d", "message": "hi"})
    assert r.get_json()["priority"] == "normal"
    assert calls[0][2] == "normal"


def test_rest_get_accepts_priority():
    client, calls = _server(lambda *a: True)
    r = client.get("/send_message?to=u@d&message=hi&priority=low")
    assert r.status_code == 200 and calls[0][2] == "low"


def test_rest_invalid_priority_is_400_and_nothing_sent():
    with tempfile.TemporaryDirectory() as tmp:
        logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
        client, calls = _server(lambda *a: True, logger)
        r = client.post("/send_message", json={"to": "u@d", "message": "hi", "priority": "urgent"})
        assert r.status_code == 400
        assert calls == []


def test_rest_handler_failure_marks_failed_and_returns_id():
    with tempfile.TemporaryDirectory() as tmp:
        logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
        client, _ = _server(lambda *a: False, logger)
        r = client.post("/send_message", json={"to": "u@d", "message": "hi"})
        assert r.status_code == 500
        assert logger.get_message(r.get_json()["message_id"])["status"] == "failed"


def test_rest_priority_not_leaked_into_fallback_text():
    client, calls = _server(lambda *a: True)
    client.post("/send_message", json={"to": "u@d", "priority": "high"})
    assert "priority" not in calls[0][1]


def test_rest_two_handler_style_lambda_still_works():
    client, _ = _server(lambda *_: True)  # как в tests/test_rest_auth.py
    assert client.post("/send_message", json={"to": "u@d", "message": "hi"}).status_code == 200


def test_broadcast_unique_ids_and_priority():
    with tempfile.TemporaryDirectory() as tmp:
        logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
        client, calls = _server(lambda *a: True, logger)
        r = client.post("/broadcast", json={"recipients": ["a@d", "b@d", "c@d"],
                                            "message": "all", "priority": "high"})
        body = r.get_json()
        ids = [x["message_id"] for x in body["results"]]
        assert len(set(ids)) == 3 and body["priority"] == "high"
        assert [c[2] for c in calls] == ["high"] * 3
        assert all(logger.get_message(i)["priority"] == "high" for i in ids)


def test_broadcast_rejects_string_recipients():
    client, _ = _server(lambda *a: True)
    r = client.post("/broadcast", json={"recipients": "abc", "message": "x"})
    assert r.status_code == 400


def test_native_log_updates_queued_row_not_duplicates():
    """REST создаёт queued, native потом логирует SENT с тем же id -> одна строка, sent."""
    with tempfile.TemporaryDirectory() as tmp:
        logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
        client, _ = _server(lambda *a: True, logger)
        mid = client.post("/send_message", json={"to": "u", "message": "hi", "priority": "high"}).get_json()["message_id"]
        logger.log_message("SENT", "srv@d/res", "u@d", "hi", message_id=mid, priority="high")
        m = logger.get_message(mid)
        assert m["status"] == "sent" and m["recipient"] == "u@d" and m["sender"] == "srv@d/res"

