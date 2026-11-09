# Контракт сообщений v1.0 (сервер мессенджера)

Нормативное описание. Расширяет `stage1_contract.md` (приоритеты, статусы, `message_id`) и
реализует ТЗ-1, ТЗ-6…10. Все пути доступны с префиксом `/v1` (`/v1/send_message` и т. д.)
и без него.

## 1. Модель сообщения

| Поле | Тип | Правило |
|---|---|---|
| `message_id` | UUID4 | присваивает сервер, сквозной (БД сервера = Room на ТСД) |
| `external_id` | строка ≤ 64 | необязательно; ключ идемпотентности от внешней системы |
| `to` | JID или логин | также принимаются `recipient`, `user`, `username`, `jid`, `send_to`, `target` |
| `type` | `text` / `task` / `command` | по умолчанию `text` |
| `priority` | `low` / `normal` / `high` / `critical` | по умолчанию `normal` |
| `message` | строка ≤ 256 символов | также `msg`, `text`, `body`, `content`; для `task`/`command` можно опустить — сервер подставит читаемый текст |
| `payload` | объект | обязателен для `task` и `command`, запрещён для `text` |

### payload для `type=task` (ТЗ-7…10)

| Поле | Обязательно | Правило |
|---|---|---|
| `kind` | да | `pick_store` (наборка в магазине), `receive` (приёмка), `pick_wh` (наборка на складе), `inventory` (ревизия) |
| `task_id` | да | строка ≤ 64 |
| `subtype` | нет | только для `inventory`: латиница/цифры/`_`/`-`, ≤ 32 |
| `params` | нет | до 10 пар «имя — значение»; имя `[A-Za-z0-9_]{1,32}`, значение строка ≤ 64 |

Имена параметров (`store`, `doc`, `zone` и т. п.) **предложены** и должны быть подтверждены командой WMS.

### payload для `type=command`

`name`: `ping`, `restart`, `update`, `notification`. Для `update` и `notification` обязателен
`arg` (≤ 128), для остальных запрещён.

### XMPP

```xml
<message to="user@domain" type="chat" id="6f1c…">
  <body>Наборка в магазине № 12345</body>
  <request xmlns="urn:xmpp:receipts"/>
  <markable xmlns="urn:xmpp:chat-markers:0"/>
  <priority xmlns="urn:wakeup:msg:0" level="high"/>
  <task xmlns="urn:wakeup:msg:0" kind="pick_store" id="12345">
    <param name="store" value="17"/>
  </task>
</message>
```

Для `command`: `<command xmlns="urn:wakeup:msg:0" name="update" arg="1.26.80"/>`.
Клиенты без поддержки нового элемента показывают `<body>` как обычный текст.

## 2. Отправка

`POST /send_message` (путь настраивается `rest_api.endpoint`):

```json
{"to": "user@arsenal", "type": "task", "priority": "high", "external_id": "ORD-7781",
 "payload": {"kind": "pick_store", "task_id": "12345", "params": {"store": "17"}}}
```

Ответ `200`: `status`, `message_id`, `priority`, `message_status` (`queued`), `type`, `to`, `message`,
`external_id` и `payload` (если были), `send_time`.

**Идемпотентность.** Повторный запрос с тем же `external_id` не отправляет сообщение заново:
ответ `200` с тем же `message_id` и `"duplicate": true`.

Если текста нет (и это `type=text`) — `400`. Прежнее поведение (тело запроса как текст) включается
только настройкой `rest_api.legacy_payload_as_message`.

`POST /broadcast`: `recipients` (список строк, не больше `broadcast_max_recipients`, по умолчанию 200),
остальные поля как выше. `external_id` рассылки действует отдельно для каждого получателя.
Ответ: `total`, `success`, `failed`, `duplicates`, `results[]`.

## 3. Статус

- `GET /messages/{message_id}` — `message_id`, `recipient`, `priority`, `status`, `send_time`,
  `delivery_time`, `read_time`.
- `POST /messages/status` — `{"ids": ["…", …]}` (до 100) → `messages[]` и `not_found[]`.
- `GET /get_messages` — список с фильтрами `limit`, `type`, `status`, `priority`.

Статусы: `queued → sent → delivered → read`, ветка `failed`. Назад статус не откатывается.
Передача наружу — pull (опрос). Push (webhook/Kafka) не реализован.

## 4. Ошибки

```json
{"error": {"code": "bad_request", "message": "…"}, "message_id": "…"}
```

Коды: `bad_request` (400), `unauthorized` (401), `admin_disabled` (403), `not_found` (404),
`method_not_allowed` (405), `rate_limited` (429), `internal` (500). Поля вроде `message_id`
передаются рядом с `error`.

## 5. Авторизация и лимиты

- Ключ — заголовок `X-API-Key` (если `rest_api.api_key` задан). Передача в `?api_key=` включается
  `rest_api.allow_api_key_query`.
- Лимит: 100 запросов в минуту на IP для отправки, рассылки, пакетного статуса и эндпоинтов терминалов.

## 6. Эндпоинты для ТСД

| Запрос | Ответ |
|---|---|
| `GET /v1/auth/xmpp?deviceId=…` | `xmppHost`, `xmppPort`, `xmppLogin`, `xmppPassword`, `apiHost`, `apiPort` |
| `POST /v1/wakeup/confirm?deviceId=…&taskId=…` | `{"status": "ok"}` |
| `GET /v1/config/trusted-senders?deviceId=…` | `{"senders": ["server@domain", …]}` |

Учётные данные выдаются только зарегистрированному активному терминалу, у которого к реестру привязана
учётная запись; иначе `404`. Если у терминала задан токен, нужен заголовок `X-Device-Token` (`401` при
несовпадении). `xmppHost`/`xmppPort` берутся из `xmpp.server`/`xmpp.port`; `apiHost`/`apiPort` — из
`rest_api.public_host`/`public_port` (если пусто — хост запроса и `rest_api.port`).
Список `senders` задаётся `rest_api.trusted_senders`.

### Административный API реестра (нужен `api_key`, иначе `403`)

- `GET /v1/admin/devices`, `GET /v1/admin/devices/{deviceId}`
- `PUT /v1/admin/devices/{deviceId}` — `xmpp_login`, `xmpp_password`, `active`, `token`, `generate_token`.
  `xmpp_login: ""` отвязывает учётную запись. Сгенерированный токен показывается один раз.
- `DELETE /v1/admin/devices/{deviceId}`

Пароли хранятся зашифрованными (Fernet). Ключ — переменная `DEVICE_REGISTRY_KEY`, иначе файл
`device_registry.key` рядом с БД (не коммитить, держать на томе).

## 7. Настройки (`rest_api`)

| Ключ | По умолчанию | Назначение |
|---|---|---|
| `allow_api_key_query` | `false` | принимать `?api_key=` |
| `legacy_payload_as_message` | `false` | старое поведение при отсутствии текста |
| `broadcast_max_recipients` | `200` | размер списка рассылки |
| `trusted_senders` | `[]` | ответ `/v1/config/trusted-senders` |
| `public_host`, `public_port` | пусто, `0` | `apiHost`, `apiPort` в `/v1/auth/xmpp` |

Каждую можно переопределить переменной окружения (`REST_ALLOW_API_KEY_QUERY`,
`REST_LEGACY_PAYLOAD_AS_MESSAGE`, `REST_BROADCAST_MAX_RECIPIENTS`, `REST_TRUSTED_SENDERS` — через запятую,
`REST_PUBLIC_HOST`, `REST_PUBLIC_PORT`).

## 8. Схема БД (SQLite, миграция v3)

`messages` + `external_id` (уникален среди исходящих), `content_type`, `payload` (JSON);
таблицы `devices` и `wakeup_confirms`. Перед миграцией создаётся копия БД `<db>.bak-v<версия>`.
