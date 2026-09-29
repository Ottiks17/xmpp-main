import os
import sqlite3
import tempfile

import pytest

from app.storage import SCHEMA_VERSION, MessageLogger, migrate


def _make_v0_db(path):
    """БД в формате до версионирования: без priority/status, user_version=0."""
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, sender TEXT NOT NULL,
            recipient TEXT NOT NULL, message TEXT NOT NULL, message_id TEXT,
            send_time TEXT, delivery_time TEXT, read_time TEXT, log_time TEXT NOT NULL)"""
    )
    rows = [
        ("SENT", "a@b", "c@d", "one", "m1", "2026-09-01T10:00:00", None, None, "2999-01-01T00:00:00"),
        ("SENT", "a@b", "c@d", "two", "m2", "2026-09-01T10:01:00", "2026-09-01T10:01:02", None, "2999-01-01T00:00:00"),
        ("SENT", "a@b", "c@d", "three", "m3", "2026-09-01T10:02:00", "2026-09-01T10:02:02", "2026-09-01T10:02:09", "2999-01-01T00:00:00"),
        ("RECEIVED", "c@d", "a@b", "hi", "r1", None, "2026-09-01T10:03:00", None, "2999-01-01T00:00:00"),
    ]
    conn.executemany(
        "INSERT INTO messages (type,sender,recipient,message,message_id,send_time,delivery_time,read_time,log_time)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_migration_keeps_data_and_backfills_status():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "old.db")
        _make_v0_db(db)
        assert migrate(db) == SCHEMA_VERSION

        conn = sqlite3.connect(db)
        rows = conn.execute("SELECT message_id, priority, status FROM messages ORDER BY id").fetchall()
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        conn.close()
        assert rows == [
            ("m1", "normal", "sent"),
            ("m2", "normal", "delivered"),
            ("m3", "normal", "read"),
            ("r1", "normal", None),
        ]
        assert os.path.exists(db + ".bak-v0")


def test_migration_is_idempotent():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "x.db")
        _make_v0_db(db)
        migrate(db)
        migrate(db)
        assert len([f for f in os.listdir(tmp) if ".bak-" in f]) == 1


def test_fresh_db_has_no_backup():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "new.db")
        MessageLogger(db_path=db)
        assert not [f for f in os.listdir(tmp) if ".bak-" in f]


def test_newer_db_is_refused():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "future.db")
        conn = sqlite3.connect(db)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
        conn.execute("CREATE TABLE t(x)")
        conn.commit()
        conn.close()
        with pytest.raises(RuntimeError):
            migrate(db)


def test_log_and_mark_read():
    with tempfile.TemporaryDirectory() as tmp:
        logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
        logger.log_message("SENT", "a@b", "c@d", "hi", message_id="msg-1", priority="high")
        logger.mark_delivered("msg-1")
        logger.mark_read("msg-1")
        m = logger.get_message("msg-1")
        assert m["status"] == "read" and m["priority"] == "high"
        assert m["delivery_time"] and m["read_time"]


def test_status_never_goes_backwards():
    with tempfile.TemporaryDirectory() as tmp:
        logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
        logger.log_message("SENT", "a@b", "c@d", "hi", message_id="m", status="queued")
        logger.mark_read("m")            # displayed пришёл раньше received
        logger.mark_delivered("m")
        assert logger.get_message("m")["status"] == "read"


def test_queued_row_is_updated_not_duplicated():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "t.db")
        logger = MessageLogger(db_path=db)
        logger.log_message("SENT", "api", "c@d", "hi", message_id="m", priority="critical", status="queued")
        from datetime import datetime
        logger.log_message("SENT", "a@b/res", "c@d", "hi", message_id="m", send_time=datetime.now())
        conn = sqlite3.connect(db)
        assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 1
        conn.close()
        m = logger.get_message("m")
        assert m["status"] == "sent" and m["priority"] == "critical" and m["send_time"]


def test_mark_failed_only_before_delivery():
    with tempfile.TemporaryDirectory() as tmp:
        logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
        logger.log_message("SENT", "a", "b", "x", message_id="q", status="queued")
        logger.log_message("SENT", "a", "b", "y", message_id="d", status="queued")
        logger.mark_delivered("d")
        logger.mark_failed("q")
        logger.mark_failed("d")
        assert logger.get_message("q")["status"] == "failed"
        assert logger.get_message("d")["status"] == "delivered"


def test_unknown_priority_rejected():
    with tempfile.TemporaryDirectory() as tmp:
        logger = MessageLogger(db_path=os.path.join(tmp, "t.db"))
        with pytest.raises(ValueError):
            logger.log_message("SENT", "a", "b", "x", message_id="z", priority="urgent")


def test_get_message_unknown_returns_none():
    with tempfile.TemporaryDirectory() as tmp:
        assert MessageLogger(db_path=os.path.join(tmp, "t.db")).get_message("nope") is None

