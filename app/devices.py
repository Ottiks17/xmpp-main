"""Реестр терминалов (ТЗ-1, пп. 11–13): deviceId -> XMPP-аккаунт.

Пароль XMPP хранится в БД только в зашифрованном виде (Fernet, AES-128-CBC + HMAC).
Ключ берётся из переменной окружения DEVICE_REGISTRY_KEY; если её нет, создаётся
файл device_registry.key рядом с БД (в Docker — на смонтированном томе).
Токен устройства (необязательный) хранится как SHA-256-хэш и проверяется в постоянное время.
"""
import hashlib
import hmac
import os
import secrets
import sqlite3
from contextlib import closing
from datetime import datetime
from threading import Lock

from cryptography.fernet import Fernet, InvalidToken

from app.constants import MESSAGES_DB_PATH

_DEVICE_ID_MAX = 128


def validate_device_id(value) -> str:
    text = str(value or "").strip()
    if not text or len(text) > _DEVICE_ID_MAX or not text.isprintable():
        raise ValueError(f"deviceId: от 1 до {_DEVICE_ID_MAX} печатных символов")
    return text


def _load_key(db_path: str) -> bytes:
    env = os.getenv("DEVICE_REGISTRY_KEY")
    if env:
        return env.encode("ascii")
    key_path = os.path.join(os.path.dirname(db_path) or ".", "device_registry.key")
    if os.path.exists(key_path):
        with open(key_path, "rb") as handle:
            return handle.read().strip()
    os.makedirs(os.path.dirname(key_path) or ".", exist_ok=True)
    key = Fernet.generate_key()
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(key)
    return key


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class DeviceRegistry:
    """Таблицы devices и wakeup_confirms создаёт миграция v3 (app.storage)."""

    def __init__(self, db_path: str = MESSAGES_DB_PATH, key: bytes | None = None):
        self.db_path = db_path
        self.lock = Lock()
        self._fernet = Fernet(key or _load_key(db_path))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5)
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    @staticmethod
    def _public(row) -> dict:
        device_id, login, active, has_token, created_at, updated_at = row
        return {
            "device_id": device_id,
            "xmpp_login": login or None,
            "active": bool(active),
            "has_token": bool(has_token),
            "created_at": created_at,
            "updated_at": updated_at,
        }

    _SELECT = (
        "SELECT device_id, xmpp_login, active, token_hash IS NOT NULL, created_at, updated_at "
        "FROM devices"
    )

    def upsert(self, device_id: str, xmpp_login: str | None = None, xmpp_password: str | None = None,
               active: bool | None = None, token: str | None = None,
               generate_token: bool = False) -> tuple[dict, str | None]:
        """Создаёт или обновляет терминал. Возвращает (запись без секретов, сгенерированный токен).

        xmpp_login="" — отвязка (учётные данные стираются). Поля, которые не переданы (None),
        не меняются."""
        device_id = validate_device_id(device_id)
        now = datetime.now().isoformat()
        new_token = secrets.token_urlsafe(24) if generate_token else None
        token = new_token or token
        with self.lock, closing(self._connect()) as conn:
            exists = conn.execute("SELECT 1 FROM devices WHERE device_id = ?", (device_id,)).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO devices (device_id, active, created_at, updated_at) VALUES (?, 1, ?, ?)",
                    (device_id, now, now),
                )
            if xmpp_login is not None:
                if xmpp_login.strip() == "":
                    conn.execute(
                        "UPDATE devices SET xmpp_login = NULL, xmpp_password_enc = NULL WHERE device_id = ?",
                        (device_id,),
                    )
                else:
                    if not xmpp_password:
                        raise ValueError("xmpp_password обязателен вместе с xmpp_login")
                    enc = self._fernet.encrypt(xmpp_password.encode("utf-8")).decode("ascii")
                    conn.execute(
                        "UPDATE devices SET xmpp_login = ?, xmpp_password_enc = ? WHERE device_id = ?",
                        (xmpp_login.strip(), enc, device_id),
                    )
            if active is not None:
                conn.execute("UPDATE devices SET active = ? WHERE device_id = ?", (1 if active else 0, device_id))
            if token is not None:
                conn.execute(
                    "UPDATE devices SET token_hash = ? WHERE device_id = ?",
                    (_hash_token(token) if token else None, device_id),
                )
            conn.execute("UPDATE devices SET updated_at = ? WHERE device_id = ?", (now, device_id))
            conn.commit()
            row = conn.execute(self._SELECT + " WHERE device_id = ?", (device_id,)).fetchone()
        return self._public(row), new_token

    def get(self, device_id: str) -> dict | None:
        with self.lock, closing(self._connect()) as conn:
            row = conn.execute(self._SELECT + " WHERE device_id = ?", (device_id,)).fetchone()
        return self._public(row) if row else None

    def list(self) -> list[dict]:
        with self.lock, closing(self._connect()) as conn:
            rows = conn.execute(self._SELECT + " ORDER BY device_id").fetchall()
        return [self._public(r) for r in rows]

    def delete(self, device_id: str) -> bool:
        with self.lock, closing(self._connect()) as conn:
            cur = conn.execute("DELETE FROM devices WHERE device_id = ?", (device_id,))
            conn.commit()
            return cur.rowcount > 0

    def credentials(self, device_id: str) -> tuple[str, str] | None:
        """(login, password) активного терминала с привязанной учётной записью, иначе None."""
        with self.lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT xmpp_login, xmpp_password_enc FROM devices WHERE device_id = ? AND active = 1",
                (device_id,),
            ).fetchone()
        if not row or not row[0] or not row[1]:
            return None
        try:
            return row[0], self._fernet.decrypt(row[1].encode("ascii")).decode("utf-8")
        except InvalidToken:
            return None  # ключ сменили — пароль не расшифровать, считаем учётную запись не привязанной

    def verify_token(self, device_id: str, token: str | None) -> bool:
        """True, если у терминала нет токена, либо токен совпал."""
        with self.lock, closing(self._connect()) as conn:
            row = conn.execute("SELECT token_hash FROM devices WHERE device_id = ?", (device_id,)).fetchone()
        if not row or not row[0]:
            return True
        return bool(token) and hmac.compare_digest(_hash_token(token), row[0])

    def record_confirm(self, device_id: str, task_id: str) -> None:
        with self.lock, closing(self._connect()) as conn:
            conn.execute(
                "INSERT INTO wakeup_confirms (device_id, task_id, confirmed_at) VALUES (?, ?, ?)",
                (device_id, task_id, datetime.now().isoformat()),
            )
            conn.commit()
