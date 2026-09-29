"""SQLite-хранилище сообщений с версионированными миграциями (PRAGMA user_version).

Схема:
  v1 — исходная (id, type, sender, recipient, message, message_id, send_time,
       delivery_time, read_time, log_time) + индекс по message_id.
  v2 — priority (low/normal/high/critical) и status (queued/sent/delivered/read/failed).
"""
import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from threading import Lock

from app.constants import DEFAULT_LOG_RETENTION_DAYS, MESSAGES_DB_PATH
from app.validation import DEFAULT_PRIORITY, normalize_priority

# Порядок статусов: назад по нему статус не откатывается (read не станет delivered).
STATUS_RANK = {"queued": 0, "sent": 1, "delivered": 2, "read": 3}
STATUS_FAILED = "failed"


def _migrate_v1(conn: sqlite3.Connection) -> None:
    """Исходная схема. Для старых БД (user_version=0) это no-op + добор message_id."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            sender TEXT NOT NULL,
            recipient TEXT NOT NULL,
            message TEXT NOT NULL,
            message_id TEXT,
            send_time TEXT,
            delivery_time TEXT,
            read_time TEXT,
            log_time TEXT NOT NULL
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)")}
    if "message_id" not in columns:
        conn.execute("ALTER TABLE messages ADD COLUMN message_id TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id)")


def _migrate_v2(conn: sqlite3.Connection) -> None:
    """priority + status. Существующие строки сохраняются, статус вычисляется по временным меткам."""
    columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)")}
    if "priority" not in columns:
        conn.execute(
            f"ALTER TABLE messages ADD COLUMN priority TEXT NOT NULL DEFAULT '{DEFAULT_PRIORITY}'"
        )
    if "status" not in columns:
        conn.execute("ALTER TABLE messages ADD COLUMN status TEXT")
        conn.execute(
            """
            UPDATE messages SET status = CASE
                WHEN read_time IS NOT NULL THEN 'read'
                WHEN delivery_time IS NOT NULL THEN 'delivered'
                ELSE 'sent'
            END
            WHERE type = 'SENT'
            """
        )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_priority ON messages(priority)")


# (версия, функция). Новые миграции — только добавлять в конец.
MIGRATIONS = [(1, _migrate_v1), (2, _migrate_v2)]
SCHEMA_VERSION = MIGRATIONS[-1][0]


def _backup(db_path: str, old_version: int) -> str:
    target = f"{db_path}.bak-v{old_version}"
    if os.path.exists(target):
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        target = f"{db_path}.bak-v{old_version}-{stamp}"
    with closing(sqlite3.connect(db_path)) as src, closing(sqlite3.connect(target)) as dst:
        src.backup(dst)
    return target


def migrate(db_path: str) -> int:
    """Приводит БД к актуальной схеме. Возвращает итоговую версию.

    Перед миграцией существующей БД делается копия <db>.bak-v<старая версия>.
    Каждая миграция выполняется в своей транзакции вместе с обновлением user_version:
    при ошибке БД остаётся на предыдущей версии.
    """
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    existed = os.path.exists(db_path) and os.path.getsize(db_path) > 0
    with closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        current = conn.execute("PRAGMA user_version").fetchone()[0]
        if current > SCHEMA_VERSION:
            raise RuntimeError(
                f"БД версии {current} новее приложения (поддерживается {SCHEMA_VERSION})"
            )
        pending = [(v, fn) for v, fn in MIGRATIONS if v > current]
        if not pending:
            return current
        if existed and current > 0:
            _backup(db_path, current)
        elif existed:
            # user_version=0, но файл не пустой: БД от старой версии приложения
            _backup(db_path, 0)
        for version, fn in pending:
            conn.execute("BEGIN IMMEDIATE")
            try:
                fn(conn)
                conn.execute(f"PRAGMA user_version = {version}")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        conn.execute("PRAGMA journal_mode=WAL")
        return SCHEMA_VERSION


_COLUMNS = (
    "id, type, sender, recipient, message, message_id, priority, status, "
    "send_time, delivery_time, read_time, log_time"
)


def _row_to_dict(cursor: sqlite3.Cursor, row) -> dict:
    return {d[0]: row[i] for i, d in enumerate(cursor.description)}


class MessageLogger:
    def __init__(self, db_path: str = MESSAGES_DB_PATH, retention_days: int = DEFAULT_LOG_RETENTION_DAYS):
        self.db_path = db_path
        self.retention_days = retention_days
        self.lock = Lock()
        self._init_database()
        self.clean_old_logs()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5)
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def _init_database(self) -> None:
        with self.lock:
            migrate(self.db_path)

    def clean_old_logs(self, days: int | None = None) -> int:
        days = days if days is not None else self.retention_days
        try:
            with self.lock, closing(self._connect()) as conn:
                old_date = (datetime.now() - timedelta(days=days)).isoformat()
                cur = conn.execute("DELETE FROM messages WHERE log_time < ?", (old_date,))
                conn.commit()
                return cur.rowcount
        except OSError:
            return 0

    def log_message(
        self,
        msg_type: str,
        sender: str,
        recipient: str,
        message: str,
        send_time: datetime | None = None,
        delivery_time: datetime | None = None,
        read_time: datetime | None = None,
        message_id: str | None = None,
        priority: str | None = None,
        status: str | None = None,
    ) -> None:
        """Пишет строку. Для SENT с уже существующим message_id строка не дублируется,
        а обновляется (REST создаёт её заранее со статусом queued)."""
        priority = normalize_priority(priority)
        if status is None and msg_type == "SENT":
            status = "sent"
        with self.lock, closing(self._connect()) as conn:
            if msg_type == "SENT" and message_id:
                row = conn.execute(
                    "SELECT id, status FROM messages WHERE message_id = ? AND type = 'SENT' "
                    "ORDER BY id LIMIT 1",
                    (message_id,),
                ).fetchone()
                if row:
                    new_status = row[1] if _rank(row[1]) > _rank(status) else status
                    conn.execute(
                        "UPDATE messages SET sender = ?, recipient = ?, "
                        "send_time = COALESCE(?, send_time), status = ? WHERE id = ?",
                        (sender, recipient, send_time.isoformat() if send_time else None, new_status, row[0]),
                    )
                    conn.commit()
                    return
            conn.execute(
                """
                INSERT INTO messages
                (type, sender, recipient, message, message_id, priority, status,
                 send_time, delivery_time, read_time, log_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    msg_type, sender, recipient, message, message_id, priority, status,
                    send_time.isoformat() if send_time else None,
                    delivery_time.isoformat() if delivery_time else None,
                    read_time.isoformat() if read_time else None,
                    datetime.now().isoformat(),
                ),
            )
            conn.commit()

    def _advance(self, message_id: str, new_status: str, column: str, when: datetime | None) -> None:
        if not message_id:
            return
        when = when or datetime.now()
        with self.lock, closing(self._connect()) as conn:
            row = conn.execute(
                f"SELECT id, status, {column} FROM messages "
                "WHERE message_id = ? AND type = 'SENT' ORDER BY id LIMIT 1",
                (message_id,),
            ).fetchone()
            if not row:
                return
            row_id, status, ts = row
            status_sql = new_status if _rank(new_status) > _rank(status) else status
            conn.execute(
                f"UPDATE messages SET {column} = COALESCE({column}, ?), status = ? WHERE id = ?",
                (when.isoformat(), status_sql, row_id),
            )
            conn.commit()

    def mark_delivered(self, message_id: str, delivery_time: datetime | None = None) -> None:
        self._advance(message_id, "delivered", "delivery_time", delivery_time)

    def mark_read(self, message_id: str, read_time: datetime | None = None) -> None:
        self._advance(message_id, "read", "read_time", read_time)

    def mark_failed(self, message_id: str) -> None:
        if not message_id:
            return
        with self.lock, closing(self._connect()) as conn:
            conn.execute(
                "UPDATE messages SET status = 'failed' "
                "WHERE message_id = ? AND type = 'SENT' AND status IN ('queued', 'sent')",
                (message_id,),
            )
            conn.commit()

    def get_message(self, message_id: str) -> dict | None:
        """Точечный статус исходящего сообщения по ID (для GET /messages/{id})."""
        if not message_id:
            return None
        with self.lock, closing(self._connect()) as conn:
            cur = conn.execute(
                f"SELECT {_COLUMNS} FROM messages WHERE message_id = ? AND type = 'SENT' "
                "ORDER BY id LIMIT 1",
                (message_id,),
            )
            row = cur.fetchone()
            return _row_to_dict(cur, row) if row else None

    def list_messages(
        self,
        limit: int = 100,
        msg_type: str | None = None,
        status: str | None = None,
        priority: str | None = None,
    ) -> list[dict]:
        """Последние сообщения (новые первыми) с необязательными фильтрами."""
        limit = max(1, min(int(limit), 1000))  # отрицательный LIMIT в SQLite = «без лимита»
        where, params = [], []
        if msg_type:
            where.append("type = ?")
            params.append(msg_type.upper())
        if status:
            where.append("status = ?")
            params.append(status.lower())
        if priority:
            where.append("priority = ?")
            params.append(normalize_priority(priority))
        sql = f"SELECT {_COLUMNS} FROM messages"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self.lock, closing(self._connect()) as conn:
            cur = conn.execute(sql, params)
            return [_row_to_dict(cur, r) for r in cur.fetchall()]

    def export_to_json(self, filepath: str = "logs/messages_export.json") -> str:
        with self.lock, closing(self._connect()) as conn:
            cur = conn.execute("SELECT * FROM messages ORDER BY id")
            messages = [_row_to_dict(cur, r) for r in cur.fetchall()]
        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as handle:
            json.dump(messages, handle, ensure_ascii=False, indent=2)
        return filepath


def _rank(status: str | None) -> int:
    return STATUS_RANK.get(status, -1)

