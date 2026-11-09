"""Контракт v1.0 (ТЗ-1, 6–10): external_id, type/payload, пакетный статус, единый формат
ошибок, лимиты рассылки, реестр терминалов и эндпоинты для ТСД."""
import json
import os
import sqlite3
import tempfile
import uuid

import pytest

from app.devices import DeviceRegistry
from app.storage import MessageLogger, migrate
from app.validation import (
    default_body, normalize_message_type, validate_external_id, validate_payload,
)
from app.xmpp.native import SimpleXMPPClient, build_payload_xml
from rest_server import RESTServer


def _server(tmp, handler=None, api_key="", config_extra=None, **rest):
    calls = []

    def wrapped(*args):
        calls.append(args)
        return True if handler is None else handler(*args)

    cfg = {"host": "127.0.0.1", "port": 8080, "endpoint": "/send_message",
           "api_key": api_key, "allow_get": False}
    cfg.update(rest)
    config = {"rest_api": cfg, "xmpp": {"server": "192.168.2.201", "port": 5222}}
    config.update(config_extra or {})
    logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
    server = RESTServer(config, message_handler=wrapped, message_logger=logger)
    return server, server.app.test_client(), calls, logger


# ------------------------------------------------------------------ валидация
def test_external_id_rules():
    assert validate_external_id(None) is None and validate_external_id("  ") is None
    assert validate_external_id(" ORD-1 ") == "ORD-1"
    with pytest.raises(ValueError):
        validate_external_id("x" * 65)


def test_message_type_default_and_unknown():
    assert normalize_message_type(None) == "text"
    assert normalize_message_type("TASK") == "task"
    with pytest.raises(ValueError):
        normalize_message_type("sms")


def test_task_payload_normalized():
    p = validate_payload("task", {"kind": "pick_store", "task_id": 12345, "params": {"store": 17}})
    assert p == {"kind": "pick_store", "task_id": "12345", "params": {"store": "17"}}
    assert default_body("task", p) == "Наборка в магазине № 12345"


def test_task_payload_rejects_bad_input():
    for bad in (
        {"kind": "unknown", "task_id": "1"},
        {"kind": "receive"},                                      # нет task_id
        {"kind": "receive", "task_id": "1", "subtype": "x"},      # subtype только для inventory
        {"kind": "receive", "task_id": "1", "params": {"bad key": "v"}},
        {"kind": "receive", "task_id": "1", "params": {f"k{i}": "v" for i in range(11)}},
    ):
        with pytest.raises(ValueError):
            validate_payload("task", bad)
    with pytest.raises(ValueError):
        validate_payload("text", {"kind": "receive"})


def test_inventory_subtype_and_command_payload():
    assert validate_payload("task", {"kind": "inventory", "task_id": "R7", "subtype": "full"})["subtype"] == "full"
    assert validate_payload("command", {"name": "ping"}) == {"name": "ping"}
    assert validate_payload("command", {"name": "update", "arg": "1.26.80"})["arg"] == "1.26.80"
    with pytest.raises(ValueError):
        validate_payload("command", {"name": "update"})          # нужен arg
    with pytest.raises(ValueError):
        validate_payload("command", {"name": "ping", "arg": "x"})


# ------------------------------------------------------------------ XML
def test_task_xml_escapes_and_has_params():
    xml = build_payload_xml("task", {"kind": "pick_store", "task_id": '12"3', "params": {"store": "A&B"}})
    assert xml == ('<task xmlns="urn:wakeup:msg:0" kind="pick_store" id="12&quot;3">'
                   '<param name="store" value="A&amp;B"/></task>')
    assert build_payload_xml("text", None) == ""
    assert build_payload_xml("command", {"name": "update", "arg": "1.2"}) == \
        '<command xmlns="urn:wakeup:msg:0" name="update" arg="1.2"/>'


def test_native_sends_task_element():
    c = SimpleXMPPClient("a@b", "pw", "srv")
    c.is_connected = True
    sent = []
    c._send_raw = sent.append
    payload = {"kind": "receive", "task_id": "77", "params": {"zone": "A1"}}
    assert c.send_message("u@d", "Приёмка № 77", priority="high", msg_type="task", payload=payload)
    assert '<task xmlns="urn:wakeup:msg:0" kind="receive" id="77">' in sent[0]
    assert 'level="high"' in sent[0] and "<body>" in sent[0]


# ------------------------------------------------------------------ REST: идемпотентность
def test_external_id_makes_send_idempotent():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, calls, _ = _server(tmp)
        body = {"to": "u@d", "message": "hi", "external_id": "ORD-1"}
        first = client.post("/send_message", json=body).get_json()
        second = client.post("/send_message", json=body)
        assert second.status_code == 200
        again = second.get_json()
        assert again["duplicate"] is True and again["message_id"] == first["message_id"]
        assert len(calls) == 1                                    # повторно в XMPP не уходило
        assert "duplicate" not in first


def test_same_external_id_race_is_caught_by_unique_index():
    with tempfile.TemporaryDirectory() as tmp:
        server, _, _, logger = _server(tmp)
        assert server._record_queued("m1", "u@d", "hi", "normal", external_id="X") == "ok"
        assert server._record_queued("m2", "u@d", "hi", "normal", external_id="X") == "duplicate"
        assert logger.find_by_external_id("X")["message_id"] == "m1"


def test_send_task_message_passes_type_and_payload_to_handler():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, calls, logger = _server(tmp)
        r = client.post("/send_message", json={
            "to": "u@d", "type": "task", "priority": "high",
            "payload": {"kind": "pick_store", "task_id": "12345", "params": {"store": "17"}},
        })
        assert r.status_code == 200
        body = r.get_json()
        assert body["type"] == "task" and body["message"] == "Наборка в магазине № 12345"
        to, text, prio, msg_id, msg_type, payload = calls[0]
        assert (to, prio, msg_type) == ("u@d", "high", "task")
        assert payload["kind"] == "pick_store"
        row = logger.get_message(body["message_id"])
        assert row["content_type"] == "task" and json.loads(row["payload"])["task_id"] == "12345"


def test_send_task_with_bad_payload_returns_400_error_object():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, calls, _ = _server(tmp)
        r = client.post("/send_message", json={"to": "u@d", "type": "task", "payload": {"kind": "nope"}})
        assert r.status_code == 400 and r.get_json()["error"]["code"] == "bad_request"
        assert calls == []


def test_error_format_is_uniform():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _, _ = _server(tmp, api_key="k")
        err = client.get("/get_messages").get_json()["error"]
        assert err["code"] == "unauthorized" and err["message"]
        assert client.get("/messages/nope", headers={"X-API-Key": "k"}).get_json()["error"]["code"] == "not_found"


# ------------------------------------------------------------------ REST: рассылка
def test_broadcast_recipient_list_is_limited():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, calls, _ = _server(tmp, broadcast_max_recipients=2)
        r = client.post("/broadcast", json={"recipients": ["a@d", "b@d", "c@d"], "message": "x"})
        assert r.status_code == 400 and "не более 2" in r.get_json()["error"]["message"]
        assert calls == []


def test_broadcast_is_rate_limited():
    with tempfile.TemporaryDirectory() as tmp:
        server, client, _, _ = _server(tmp)
        server.rate_limiter.max_requests = 1
        assert client.post("/broadcast", json={"recipients": ["a@d"], "message": "x"}).status_code == 200
        r = client.post("/broadcast", json={"recipients": ["a@d"], "message": "y"})
        assert r.status_code == 429 and r.get_json()["error"]["code"] == "rate_limited"


def test_broadcast_external_id_is_per_recipient_idempotent():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, calls, _ = _server(tmp)
        body = {"recipients": ["a@d", "b@d"], "message": "hi", "external_id": "B-1"}
        first = client.post("/broadcast", json=body).get_json()
        second = client.post("/broadcast", json=body).get_json()
        assert first["duplicates"] == 0 and second["duplicates"] == 2 and second["success"] == 2
        assert len(calls) == 2
        assert {r["message_id"] for r in first["results"]} == {r["message_id"] for r in second["results"]}


# ------------------------------------------------------------------ REST: пакетный статус и /v1
def test_batch_status_and_not_found():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _, logger = _server(tmp)
        ids = [client.post("/send_message", json={"to": "u@d", "message": str(i)}).get_json()["message_id"]
               for i in range(2)]
        logger.mark_delivered(ids[0])
        missing = str(uuid.uuid4())
        r = client.post("/messages/status", json={"ids": ids + [missing]})
        body = r.get_json()
        assert r.status_code == 200 and body["count"] == 2 and body["not_found"] == [missing]
        by_id = {m["message_id"]: m["status"] for m in body["messages"]}
        assert by_id[ids[0]] == "delivered" and by_id[ids[1]] == "queued"


def test_batch_status_validation():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _, _ = _server(tmp)
        assert client.post("/messages/status", json={"ids": []}).status_code == 400
        assert client.post("/messages/status", json={"ids": ["bad id!"]}).status_code == 400
        assert client.post("/messages/status", json={"ids": ["a"] * 101}).status_code == 400


def test_v1_aliases_work():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _, _ = _server(tmp)
        assert client.get("/v1/health").status_code == 200
        r = client.post("/v1/send_message", json={"to": "u@d", "message": "hi"})
        assert r.status_code == 200
        assert client.get(f"/v1/messages/{r.get_json()['message_id']}").status_code == 200


# ------------------------------------------------------------------ реестр терминалов
def test_registry_encrypts_password_at_rest():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "t.db")
        MessageLogger(db_path=db)
        reg = DeviceRegistry(db_path=db)
        reg.upsert("TSD-1", xmpp_login="ivan@arsenal", xmpp_password="s3cret-pw")
        assert reg.credentials("TSD-1") == ("ivan@arsenal", "s3cret-pw")
        raw = sqlite3.connect(db).execute("SELECT xmpp_password_enc FROM devices").fetchone()[0]
        assert "s3cret-pw" not in raw
        reg.upsert("TSD-1", xmpp_login="")                         # отвязка
        assert reg.credentials("TSD-1") is None


def test_registry_inactive_device_gets_no_credentials_and_token_check():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "t.db")
        MessageLogger(db_path=db)
        reg = DeviceRegistry(db_path=db)
        _, token = reg.upsert("TSD-2", xmpp_login="a@d", xmpp_password="p", generate_token=True)
        assert token and reg.verify_token("TSD-2", token) and not reg.verify_token("TSD-2", "wrong")
        assert not reg.verify_token("TSD-2", None)
        reg.upsert("TSD-2", active=False)
        assert reg.credentials("TSD-2") is None
        assert reg.verify_token("UNKNOWN", None)                   # нет записи -> нет токена


def test_v3_migration_keeps_old_rows():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "old.db")
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, "
                     "sender TEXT NOT NULL, recipient TEXT NOT NULL, message TEXT NOT NULL, message_id TEXT, "
                     "send_time TEXT, delivery_time TEXT, read_time TEXT, log_time TEXT NOT NULL)")
        conn.execute("INSERT INTO messages (type, sender, recipient, message, message_id, log_time) "
                     "VALUES ('SENT','a','b','old','m-old','2099-01-01T00:00:00')")
        conn.commit()
        conn.close()
        assert migrate(db) == 3 and migrate(db) == 3               # идемпотентна
        row = MessageLogger(db_path=db, retention_days=99999).get_message("m-old")
        assert row["message"] == "old" and row["content_type"] == "text" and row["external_id"] is None


# ------------------------------------------------------------------ эндпоинты для ТСД
def _admin(client, device="TSD-1", **body):
    return client.put(f"/v1/admin/devices/{device}", json=body, headers={"X-API-Key": "adm"})


def test_admin_api_requires_api_key():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _, _ = _server(tmp)                             # ключ не задан
        r = client.get("/v1/admin/devices")
        assert r.status_code == 403 and r.get_json()["error"]["code"] == "admin_disabled"
        _, client2, _, _ = _server(tempfile.mkdtemp(dir=tmp), api_key="adm")
        assert client2.get("/v1/admin/devices").status_code == 401


def test_device_flow_auth_confirm_trusted_senders():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _, _ = _server(tmp, api_key="adm", trusted_senders=["server@arsenal"],
                                  public_host="10.0.0.5", public_port=8080)
        r = _admin(client, xmpp_login="ivan@arsenal", xmpp_password="pw1")
        assert r.status_code == 200 and "xmpp_password" not in json.dumps(r.get_json())

        auth = client.get("/v1/auth/xmpp?deviceId=TSD-1")
        assert auth.status_code == 200
        assert auth.get_json() == {"xmppHost": "192.168.2.201", "xmppPort": 5222,
                                   "xmppLogin": "ivan@arsenal", "xmppPassword": "pw1",
                                   "apiHost": "10.0.0.5", "apiPort": 8080}

        assert client.post("/v1/wakeup/confirm?deviceId=TSD-1&taskId=12345").get_json() == {"status": "ok"}
        assert client.post("/v1/wakeup/confirm?deviceId=TSD-1").status_code == 400
        assert client.get("/v1/config/trusted-senders?deviceId=TSD-1").get_json() == {"senders": ["server@arsenal"]}


def test_unregistered_device_gets_404_and_missing_id_400():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _, _ = _server(tmp, api_key="adm")
        assert client.get("/v1/auth/xmpp?deviceId=NOPE").status_code == 404
        assert client.get("/v1/auth/xmpp").status_code == 400


def test_device_token_is_enforced_when_set():
    with tempfile.TemporaryDirectory() as tmp:
        _, client, _, _ = _server(tmp, api_key="adm")
        r = _admin(client, xmpp_login="ivan@arsenal", xmpp_password="pw1", generate_token=True)
        token = r.get_json()["token"]
        assert client.get("/v1/auth/xmpp?deviceId=TSD-1").status_code == 401
        ok = client.get("/v1/auth/xmpp?deviceId=TSD-1", headers={"X-Device-Token": token})
        assert ok.status_code == 200
        assert client.get("/v1/admin/devices/TSD-1", headers={"X-API-Key": "adm"}).get_json()["device"]["has_token"]
        assert client.delete("/v1/admin/devices/TSD-1", headers={"X-API-Key": "adm"}).status_code == 200
        assert client.delete("/v1/admin/devices/TSD-1", headers={"X-API-Key": "adm"}).status_code == 404
