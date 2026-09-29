# Этап 1. Контракт priority / статусов (XMPP2 ↔ WakeUpMessengerW)

## 1. Приоритет (общий словарь)
`low` | `normal` | `high` | `critical`. Нет значения или пустое → `normal`.
Неизвестное значение: REST → 400, Android при приёме → `normal` (с записью в лог).

## 2. XMPP: расширение в `<message>`
```xml
<message to="user@domain" type="chat" id="6f1c...-uuid">
  <body>Текст (до 256 символов)</body>
  <request xmlns="urn:xmpp:receipts"/>
  <markable xmlns="urn:xmpp:chat-markers:0"/>
  <priority xmlns="urn:wakeup:msg:0" level="critical"/>
</message>
```
- `id` сообщения = `message_id` в БД сервера = `id` в Room на Android (сквозной ID).
- Формат ID: UUID4 (вместо `int(time()*1000)`), чтобы `/broadcast` не давал дублей.
- Нет расширения → `normal` (старые отправители не ломаются).

## 3. REST сервера
**POST /send_message** — тело: `to`, `message`, необязательный `priority`.
Ответ 200: `{"status":"success","message_id":"...","priority":"high","message_status":"sent","to":"...","send_time":"..."}`
(при Kafka-очереди: 202, `message_status":"queued"`, тот же `message_id`).

**POST /broadcast** — то же + `priority`; в `results[]` у каждого получателя свой `message_id`.

**GET /messages/{message_id}** — точечный статус:
```json
{"status":"success","message":{
  "message_id":"...","recipient":"user@domain","priority":"high",
  "status":"delivered","send_time":"...","delivery_time":"...","read_time":null}}
```
404 — ID неизвестен. Авторизация `X-API-Key`, как у остальных.

**Статусы:** `queued → sent → delivered → read`, ответвление `failed` (только до доставки).
Статус монотонный: `read` не откатывается в `delivered`.

**/get_messages** дополняется полями `message_id`, `priority`, `status`.

## 4. Android: приоритет → поведение (Android 10, API 29)
Действие команды не зависит от приоритета: `task` всегда будит WMS, `ping` всегда тихий.
Приоритет определяет только уведомление:

| priority | канал (новый ID) | поведение |
|---|---|---|
| low | `wakeup_msg_low`, IMPORTANCE_LOW | тихо, в шторке |
| normal | `wakeup_msg_normal`, IMPORTANCE_DEFAULT | звук |
| high | `wakeup_msg_high`, IMPORTANCE_HIGH | звук + вибрация + heads-up |
| critical | `wakeup_msg_critical`, IMPORTANCE_HIGH + fullScreenIntent | Activity поверх блокировки, экран включается; звук один раз, без кнопки подтверждения |

Каналы имеют новые ID, потому что важность уже созданных каналов менять нельзя.
На API 29 `USE_FULL_SCREEN_INTENT` — обычное разрешение (достаточно манифеста).

## 5. Доверенные отправители
Список приходит через REST (эндпоинт и поле уточняем на этапе 7). До получения списка
приоритет `critical` понижается до `high`, чтобы посторонний отправитель не поднимал full-screen.

