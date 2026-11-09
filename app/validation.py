import re

from app.constants import MAX_MESSAGE_LENGTH

JID_PATTERN = re.compile(
    r"""^[^@\s/"'<>&\\]+@[^@\s/"'<>&\\]+$|^[^@\s/"'<>&\\]+$"""
)

def validate_message(text: str) -> str:
    message = (text or "").strip()
    if not message:
        raise ValueError("Сообщение не может быть пустым")
    if len(message) > MAX_MESSAGE_LENGTH:
        raise ValueError(f"Сообщение превышает {MAX_MESSAGE_LENGTH} символов")
    return message


def validate_jid(jid: str, default_domain: str) -> str:
    value = (jid or "").strip()
    if not value:
        raise ValueError("JID не указан")
    if "@" not in value:
        value = f"{value}@{default_domain}"
    if not JID_PATTERN.match(value.split("/")[0]):
        raise ValueError("Некорректный JID")
    return value.split("/")[0]


PRIORITIES = ("low", "normal", "high", "critical")
DEFAULT_PRIORITY = "normal"


def normalize_priority(value: str | None) -> str:
    """None/пустое -> normal; неизвестное значение -> ValueError (REST вернёт 400)."""
    if value is None or str(value).strip() == "":
        return DEFAULT_PRIORITY
    priority = str(value).strip().lower()
    if priority not in PRIORITIES:
        raise ValueError(f"priority должен быть одним из: {', '.join(PRIORITIES)}")
    return priority


# --------------------------------------------------------------- контракт v1.0
MESSAGE_TYPES = ("text", "task", "command")
DEFAULT_MESSAGE_TYPE = "text"

# Виды заданий (ТЗ-7…10). Список закрытый: расширяется версией контракта.
TASK_KINDS = {
    "pick_store": "Наборка в магазине",
    "receive": "Приёмка",
    "pick_wh": "Наборка на складе",
    "inventory": "Ревизия",
}
COMMAND_NAMES = ("ping", "restart", "update", "notification")
_COMMANDS_WITH_ARG = ("update", "notification")

MAX_EXTERNAL_ID_LENGTH = 64
MAX_TASK_ID_LENGTH = 64
MAX_PARAMS = 10
MAX_PARAM_VALUE_LENGTH = 64
MAX_COMMAND_ARG_LENGTH = 128
_PARAM_KEY_RE = re.compile(r"^[A-Za-z0-9_]{1,32}$")
_SUBTYPE_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


def validate_external_id(value) -> str | None:
    """None/пустое -> None. Иначе строка до 64 печатных символов без пробелов по краям."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) > MAX_EXTERNAL_ID_LENGTH or not text.isprintable():
        raise ValueError(f"external_id: до {MAX_EXTERNAL_ID_LENGTH} печатных символов")
    return text


def normalize_message_type(value) -> str:
    if value is None or str(value).strip() == "":
        return DEFAULT_MESSAGE_TYPE
    msg_type = str(value).strip().lower()
    if msg_type not in MESSAGE_TYPES:
        raise ValueError(f"type должен быть одним из: {', '.join(MESSAGE_TYPES)}")
    return msg_type


def _short_str(value, name: str, max_len: int) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{name}: ожидается строка")
    text = str(value).strip()
    if not text or len(text) > max_len or not text.isprintable():
        raise ValueError(f"{name}: от 1 до {max_len} печатных символов")
    return text


def validate_payload(msg_type: str, payload) -> dict | None:
    """Проверяет и нормализует payload для типа сообщения. Для text payload не допускается."""
    if msg_type == "text":
        if payload:
            raise ValueError("payload не используется для type=text")
        return None
    if not isinstance(payload, dict):
        raise ValueError(f"payload обязателен для type={msg_type} (объект)")

    if msg_type == "task":
        kind = str(payload.get("kind") or "").strip().lower()
        if kind not in TASK_KINDS:
            raise ValueError(f"payload.kind должен быть одним из: {', '.join(TASK_KINDS)}")
        result = {"kind": kind, "task_id": _short_str(payload.get("task_id"), "payload.task_id", MAX_TASK_ID_LENGTH)}
        subtype = payload.get("subtype")
        if subtype not in (None, ""):
            if kind != "inventory":
                raise ValueError("payload.subtype допустим только для kind=inventory")
            subtype = str(subtype).strip()
            if not _SUBTYPE_RE.match(subtype):
                raise ValueError("payload.subtype: латиница, цифры, '_' и '-', до 32 символов")
            result["subtype"] = subtype
        params = payload.get("params")
        if params not in (None, {}):
            if not isinstance(params, dict) or len(params) > MAX_PARAMS:
                raise ValueError(f"payload.params: объект не более чем из {MAX_PARAMS} полей")
            clean = {}
            for key, val in params.items():
                if not _PARAM_KEY_RE.match(str(key)):
                    raise ValueError("payload.params: имена — латиница, цифры и '_', до 32 символов")
                clean[str(key)] = _short_str(val, f"payload.params.{key}", MAX_PARAM_VALUE_LENGTH)
            result["params"] = clean
        return result

    # command
    name = str(payload.get("name") or "").strip().lower()
    if name not in COMMAND_NAMES:
        raise ValueError(f"payload.name должен быть одним из: {', '.join(COMMAND_NAMES)}")
    result = {"name": name}
    arg = payload.get("arg")
    if name in _COMMANDS_WITH_ARG:
        result["arg"] = _short_str(arg, "payload.arg", MAX_COMMAND_ARG_LENGTH)
    elif arg not in (None, ""):
        raise ValueError(f"payload.arg не используется командой {name}")
    return result


def default_body(msg_type: str, payload: dict | None) -> str:
    """Читаемый текст для уведомления, если клиент не передал message."""
    if msg_type == "task" and payload:
        return f"{TASK_KINDS[payload['kind']]} № {payload['task_id']}"
    if msg_type == "command" and payload:
        return payload.get("arg") or payload["name"]
    return ""
