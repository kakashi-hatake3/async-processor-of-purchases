# Асинхронный сервис процессинга платежей

Микросервис для асинхронного приема и обработки платежей с гарантированной доставкой событий (**Outbox pattern**), защитой от дублирования (**Idempotency Key**), эмуляцией внешнего платежного шлюза, отправкой Webhook-уведомлений клиенту с политикой повторных попыток (**Exponential Backoff Retry**) и очередью недоставленных сообщений (**Dead Letter Queue / DLQ**).

---

## 🛠 Стек технологий

* **Язык & Фреймворк:** Python 3.12, FastAPI, Pydantic v2
* **База данных & ORM:** PostgreSQL 16, SQLAlchemy 2.0 (асинхронный режим), Alembic
* **Брокер сообщений:** RabbitMQ 3 (Management), FastStream (aiopika)
* **HTTP клиент & утилиты:** HTTPX, Tenacity
* **Контейнеризация:** Docker, Docker Compose
* **Тестирование:** Pytest, Pytest-Asyncio, aiosqlite

---

## 🏛 Архитектурные особенности

```
       +-------------------------------------------------------+
       |                     Клиент / Frontend                 |
       +-------------------------------------------------------+
               │ POST /api/v1/payments                 ▲
               │ (X-API-Key, Idempotency-Key)          │ Webhook POST
               ▼                                       │ (3 retries, backoff)
       +───────────────────────────────────+           │
       │            FastAPI (API)          │           │
       │                                   │           │
       │  1. Проверка Idempotency-Key      │           │
       │  2. Транзакция:                   │           │
       │     INSERT payments (pending)     │           │
       │     INSERT outbox (payments.new)  │           │
       │  3. Ответ 202 Accepted            │           │
       +───────────────────────────────────+           │
               │                   ▲                   │
               │ Transactional DB  │                   │
               ▼                   │                   │
         [ PostgreSQL ] ───────────┘                   │
         - payments                                    │
         - outbox (FOR UPDATE SKIP LOCKED)             │
               │                                       │
               │ OutboxPublisher (background worker)   │
               ▼                                       │
       +───────────────────────────────────+           │
       │             RabbitMQ              │           │
       │                                   │           │
       │  Exchange: payments.exchange      │           │
       │  Queue: payments.new              │           │
       │  DLX: payments.dlx                │           │
       │  DLQ: payments.dlq (max retries)  │           │
       +───────────────────────────────────+           │
               │                                       │
               ▼ Consume (payments.new)                │
       +───────────────────────────────────+           │
       │       FastStream (Consumer)       │           │
       │                                   │           │
       │  1. Эмуляция шлюза (2-5 сек,      │           │
       │     90% success / 10% failed)     │           │
       │  2. UPDATE payments status in DB  │           │
       │  3. Отправка Webhook ─────────────┴───────────┘
       │  4. Retry (3 попытки) & DLQ       
       +───────────────────────────────────+
```

### 1. Гарантии доставки (Outbox Pattern)
* При создании платежа запись о платеже (`payments`) и событие для публикации (`outbox`) сохраняются **в одной атомарной транзакции базы данных**. Это исключает сценарий рассинхронизации (dual-write problem).
* Фоновый процесс `OutboxPublisher` опрашивает базу данных с использованием конструкции `SELECT ... FOR UPDATE SKIP LOCKED`, что позволяет безопасно масштабировать API на любое количество реплик без дублирования обработки.
* После успешной отправки в RabbitMQ запись в таблице `outbox` переводится в статус `processed`.

### 2. Защита от дублей (Idempotency Key)
* Каждый запрос на создание платежа требует обязательный заголовок `Idempotency-Key`.
* Если поступает запрос с уже существующим ключом и идентичными параметрами, сервис возвращает ранее созданный платеж со статусом `202 Accepted` без повторного создания события.
* Если параметры запроса отличаются от ранее зарегистрированных для этого ключа — сервис возвращает `409 Conflict`.
* На уровне PostgreSQL создано уникальное ограничение `uq_payments_idempotency_key` для защиты от race condition при параллельных запросах.

### 3. Эмуляция шлюза и Webhook
* Consumer эмулирует вызов внешнего эквайринга с задержкой 2–5 секунд (90% шанс успеха со статусом `succeeded`, 10% со статусом `failed`).
* Статус и время обработки `processed_at` обновляются в БД.
* На указанный клиентом `webhook_url` отправляется POST-запрос с деталями платежа. При недоступности сервера клиента реализуются повторные попытки (3 попытки с экспоненциальной задержкой).

### 4. Dead Letter Queue (DLQ) и Consumer Retry
* При сбоях на этапе обработки события consumer выполняет до 3 попыток обработки с экспоненциальной задержкой.
* При исчерпании 3 попыток сообщение автоматически перенаправляется в очередь недоставленных сообщений `payments.dlq` через обменник `payments.dlx` с заголовками ошибки и временной меткой.

---

## 🚀 Быстрый запуск через Docker Compose

Для запуска сервиса со всеми компонентами (PostgreSQL, RabbitMQ, API, Consumer) выполните:

```bash
docker compose up --build
```

Сервисы будут доступны по следующим адресам:
* **API эндпоинты:** `http://localhost:8000`
* **Интерактивная документация (Swagger / OpenAPI):** `http://localhost:8000/docs`
* **RabbitMQ Management UI:** `http://localhost:15672` (логин: `guest`, пароль: `guest`)
* **PostgreSQL:** `localhost:5432` (пользователь: `postgres`, пароль: `postgres`, база: `payments_db`)

---

## 🔑 Аутентификация

Все запросы к API защищены статическим API-ключом, передаваемым в заголовке `X-API-Key`.
По умолчанию:
```
X-API-Key: test-secret-api-key-12345
```
*(Значение настраивается через переменную окружения `API_KEY` в файле `.env`)*.

---

## 📖 Примеры запросов API

### 1. Создание платежа

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-secret-api-key-12345" \
  -H "Idempotency-Key: my-order-uuid-001" \
  -d '{
    "amount": "1499.50",
    "currency": "RUB",
    "description": "Оплата заказа #12345",
    "metadata": {
      "order_id": 12345,
      "customer_email": "user@example.com"
    },
    "webhook_url": "https://webhook.site/#!/view"
  }'
```

**Ответ (`202 Accepted`):**
```json
{
  "payment_id": "4b68e98e-4a69-4e78-bcce-78a08c02824b",
  "status": "pending",
  "created_at": "2026-11-04T12:00:00.123456Z"
}
```

---

### 2. Получение информации о платеже

```bash
curl -X GET http://localhost:8000/api/v1/payments/4b68e98e-4a69-4e78-bcce-78a08c02824b \
  -H "X-API-Key: test-secret-api-key-12345"
```

**Ответ (`200 OK`):**
```json
{
  "payment_id": "4b68e98e-4a69-4e78-bcce-78a08c02824b",
  "amount": "1499.50",
  "currency": "RUB",
  "description": "Оплата заказа #12345",
  "metadata": {
    "order_id": 12345,
    "customer_email": "user@example.com"
  },
  "status": "succeeded",
  "idempotency_key": "my-order-uuid-001",
  "webhook_url": "https://webhook.site/#!/view",
  "created_at": "2026-11-04T12:00:00.123456Z",
  "updated_at": "2026-11-04T12:00:03.456789Z",
  "processed_at": "2026-11-04T12:00:03.456789Z"
}
```

---

### 3. Проверка идемпотентности

Повторная отправка запроса с тем же заголовком `Idempotency-Key: my-order-uuid-001` и тем же телом возвращает существующий платеж (`202 Accepted`) без создания дубликата.

При попытке изменить тело запроса (например, передать другую сумму):
```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-secret-api-key-12345" \
  -H "Idempotency-Key: my-order-uuid-001" \
  -d '{
    "amount": "9999.00",
    "currency": "RUB",
    "description": "Измененная сумма",
    "webhook_url": "https://webhook.site/#!/view"
  }'
```

**Ответ (`409 Conflict`):**
```json
{
  "detail": "Idempotency key conflict: request payload differs from existing payment"
}
```

---

### 4. Формат Webhook-уведомления

После обработки платежа consumer отправляет POST-запрос на `webhook_url`:
```json
{
  "event": "payment.updated",
  "payment_id": "4b68e98e-4a69-4e78-bcce-78a08c02824b",
  "status": "succeeded",
  "amount": "1499.50",
  "currency": "RUB",
  "description": "Оплата заказа #12345",
  "metadata": {
    "order_id": 12345,
    "customer_email": "user@example.com"
  },
  "processed_at": "2026-11-04T12:00:03.456789Z"
}
```

---

## 🧪 Запуск тестов

Для запуска модульных и интеграционных тестов:

```bash
# Активация виртуального окружения (если используется локально)
source .venv/bin/activate

# Запуск тестов
pytest -v
```

Тестовый набор покрывает:
* Валидацию API и авторизацию `X-API-Key`
* Полноценный цикл создания и получения платежа
* Идемпотентность и обработку конфликтов (`409 Conflict`)
* Атомарное сохранение и публикацию `Outbox` сообщений
* Эмуляцию шлюза и отправку вебхуков с повторными попытками
* Consumer retry логику и перенаправление сообщений в DLQ

---

## 💻 Локальный запуск без Docker

1. **Создайте и активируйте окружение:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Запустите PostgreSQL и RabbitMQ** (или используйте локальные сервисы).

3. **Примените миграции базы данных:**
   ```bash
   alembic upgrade head
   ```

4. **Запустите API сервис:**
   ```bash
   uvicorn app.main:app --reload --port 8000
   ```

5. **В отдельном терминале запустите Consumer:**
   ```bash
   python -m app.worker
   ```

---

## 🔧 Возможные проблемы (Troubleshooting)

### Ошибка DNS при `docker build`: `write: permission denied on 127.0.0.53:53`
Если Docker установлен через **Snap** на Ubuntu, профиль безопасности Snap AppArmor блокирует запросы daemon к локальному DNS-резолверу systemd (`127.0.0.53:53`).

**Решение:**
Укажите внешние DNS-серверы в конфигурации Docker (`/var/snap/docker/current/config/daemon.json`):
```json
{
    "log-level": "error",
    "dns": ["8.8.8.8", "1.1.1.1"]
}
```
И перезапустите службу:
```bash
sudo systemctl restart snap.docker.dockerd.service
```
