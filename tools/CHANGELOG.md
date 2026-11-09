# История разовых патчей (tools/)

Скрипты `fixG.py` и `fixH.py`, применявшие точечные правки к `gui_app.py`
(отправка XEP-0333 displayed-маркера и диалог автообновления), удалены —
обе правки уже внесены в `gui_app.py` напрямую. Если понадобится похожая
разовая правка через regex-патч, используйте их как образец в истории git.

## Контракт v1.0 (ТЗ-1, 6–10)

- `external_id` (идемпотентность), `type` (`text`/`task`/`command`) и `payload` в `/send_message` и `/broadcast`.
- XMPP-элементы `<task>` и `<command>` в `urn:wakeup:msg:0` (`app/xmpp/native.py`).
- Пакетный статус `POST /messages/status`; единый формат ошибок `{"error": {"code", "message"}}`; алиасы `/v1/...`.
- `/broadcast`: лимит частоты и размера списка. Запрос без текста — 400 (старое поведение: `legacy_payload_as_message`).
- Ключ API только заголовком (`allow_api_key_query` включает `?api_key=`).
- Эндпоинты для ТСД `/v1/auth/xmpp`, `/v1/wakeup/confirm`, `/v1/config/trusted-senders` и административный
  API реестра терминалов (`app/devices.py`). Миграция БД v3.
- Описание: `contract_v1.md`.
