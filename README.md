# Order Service

Сервис заказов, реализованный по принципам **чистой архитектуры** (Clean Architecture).
Обрабатывает жизненный цикл заказа (`NEW → PAID → SHIPPED` / `CANCELLED`) и интегрируется
с сервисами Capashino: **Catalog**, **Payments**, **Shipping** (через Kafka) и
**Notifications**.

## Архитектура

Правило зависимостей: импорты направлены внутрь, к слою `domain`. Порты описаны в
`application/ports/`, их реализации — в `infrastructure/`.

```
src/
├── domain/                  # Ядро: сущности, статусы, доменные исключения и события
│   ├── entities.py          # @dataclass Order, Enum OrderStatus, машина переходов
│   ├── exceptions.py        # Доменные ошибки (InsufficientStock, InvalidTransition, ...)
│   ├── events.py            # Фабрики payload'ов событий (order.paid)
│   └── notifications.py     # Тексты уведомлений со статус-маркерами, нормализация user_id
├── application/
│   ├── ports/               # Интерфейсы: репозитории, UoW, клиенты Catalog/Payments/
│   │                        # Notifications, EventPublisher, Inbox
│   └── usecases/            # По файлу на сценарий:
│       ├── create_order.py                # Проверка стока → заказ → платёж → NEW-уведомление
│       ├── get_order.py                   # Чтение заказа
│       ├── process_payment_callback.py    # succeeded→PAID / failed→CANCELLED + outbox + UoW
│       ├── handle_shipment_event.py       # order.shipped→SHIPPED / order.cancelled→CANCELLED (Inbox)
│       ├── notify_order_status.py         # OrderNotifier: фоновая best-effort доставка
│       └── notification_dispatcher.py     # Диспетчер уведомлений статуса заказа
├── infrastructure/
│   ├── persistence/         # SQLAlchemy-модели (отдельны от домена), Repository, UoW
│   ├── http/                # Клиенты Catalog / Payments / Notifications (X-API-Key)
│   └── messaging/           # Kafka producer/consumer, OutboxWorker, Inbox
├── presentation/api/        # FastAPI: routes/, schemas.py, dependencies.py (composition root)
├── main.py                  # Фабрика create_app() + lifespan (outbox worker, consumer)
└── settings.py              # pydantic-settings: вся конфигурация из переменных окружения
alembic/                     # Миграции БД (orders, payment_callbacks, outbox_events, inbox_events)
```

### Ключевые паттерны

| Паттерн | Реализация |
|---|---|
| Repository | `OrderRepository` / `IdempotencyRepository` (`infrastructure/persistence/repositories.py`) |
| Unit of Work | `SqlAlchemyUow`: транзакция на заказ + callback + outbox-запись атомарно |
| Outbox | Таблица `outbox_events`: событие `order.paid` пишется в той же транзакции, что и переход в PAID; `OutboxWorker` публикует в Kafka (at-least-once) |
| Inbox | Таблица `inbox_events` (unique `event_id`): дубликаты событий Shipping игнорируются |
| Идемпотентность | unique `idempotency_key` на заказах, дедупликация callbacks по `payment_id`, детерминированные ключи уведомлений `{order_id}:{status}` |

## Статусы заказа

`NEW` → создан · `PAID` → платёж успешен · `SHIPPED` → отправлен · `CANCELLED` → отменён.
Переходы валидируются машиной состояний в домене (недопустимый переход — ошибка, повторный
callback идемпотентен).

## API

Все ответы/запросы — JSON. Внутренний адрес для callback'ов: `<service>.<namespace>.svc:8000`.

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/api/orders` | Создать заказ. Проверяет товар и `available_qty` через Catalog; при нехватке — `400`; создаёт платёж через Payments (при сбое платежа заказ → `CANCELLED`). Идемпотентен по `idempotency_key`. Ответ `201` |
| `GET` | `/api/orders/{order_id}` | Получить заказ. Ответ `200` |
| `POST` | `/api/orders/payment-callback` | Callback от Payments Service: `succeeded` → `PAID` (+ событие `order.paid` в outbox), `failed` → `CANCELLED`. Повторные callback'и — no-op. Ответ `200` |

Пример создания заказа:

```json
{ "user_id": "user_1", "quantity": 1, "item_id": "item-uuid", "idempotency_key": "some-uuid" }
```

## Интеграции

### Catalog Service (синхронно, HTTP)
`GET /api/catalog/items/{item_id}` с заголовком `X-API-Key`. При создании заказа проверяется
наличие товара и `available_qty >= quantity`. Ошибки сервиса транслируются в 4xx, не в 500.

### Payments Service (синхронно, HTTP + callback)
`POST /api/payments` с `order_id`, `amount`, `idempotency_key` и `callback_url`, построенным
из `ORDER_SERVICE_INTERNAL_URL` (внутренний k8s DNS, **не** внешний домен). Обработка платежа
асинхронна — результат приходит на payment-callback.

### Shipping Service (Kafka, event-driven)
- **Исходящие:** топик `student_system-order.events`, событие
  `{"event_type":"order.paid","order_id":...,"item_id":...,"quantity":...,"idempotency_key":...}`
  публикуется OutboxWorker'ом после перехода в PAID.
- **Входящие:** топик `student_system-shipment.events`, события `order.shipped` (→ `SHIPPED`)
  и `order.cancelled` (→ `CANCELLED`, причина сохраняется). Обработка через Inbox — идемпотентно.

### Notifications Service (HTTP, best-effort)
`POST /api/notifications` при каждом изменении статуса. Уведомления доставляются в фоновом
потоке и **не блокируют** основной процесс: ошибки только логируются, идут ретраи с backoff.
Тексты содержат статус-маркеры, например `[PAID] Ваш заказ успешно оплачен и готов к отправке`,
`[CANCELLED] Ваш заказ отменен. Причина: {reason}`. Идемпотентность — ключ `{order_id}:{status}`.
Поле `user_id` нормализуется к UUID (детерминированный `uuid5` для произвольных ID), как
требует текущая реализация сервиса.

## Конфигурация (переменные окружения)

Вся конфигурация читается из окружения (`src/settings.py`), ничего не хардкодится.
Полный пример — в [.env.example](.env.example).

| Переменная | Обязательно | Описание / дефолт |
|---|---|---|
| `CAPASHINO_API_TOKEN` | ✅ | Личный API-токен (вкладка «Профиль» LMS); заголовок `X-API-Key` для всех сервисов Capashino |
| `ORDER_SERVICE_INTERNAL_URL` | ✅ | Внутренний адрес сервиса для callback'ов: `http://<service-name>.<namespace>.svc:8000` |
| `KAFKA_BOOTSTRAP_SERVERS` | ✅ | `kafka.kafka.svc.cluster.local:9092` |
| `KAFKA_CONSUMER_GROUP_ID` | — | Дефолт `order-service`; в кластере задайте уникальный (`student-<user>-order-service`) |
| `CAPASHINO_INTERNAL_URL` | — | Дефолт `http://student-system-capashino-web.student-system-capashino.svc:8000` |
| `NOTIFICATIONS_BASE_URL` | — | Дефолт = хостнейм capashino |
| `NOTIFICATIONS_API_TOKEN` | — | Если пусто — используется `CAPASHINO_API_TOKEN` |
| `ORDER_EVENTS_TOPIC` / `SHIPMENT_EVENTS_TOPIC` | — | `student_system-order.events` / `student_system-shipment.events` |
| `OUTBOX_POLL_INTERVAL` | — | `1.0` сек — период опроса outbox |
| `HTTP_TIMEOUT` | — | `10.0` сек — таймаут вызовов внешних сервисов |
| `PAYMENT_CALLBACK_PATH` | — | `/api/orders/payment-callback` |
| `DEBUG` | — | `false` |

**База данных.** Приложение собирает строку подключения само из переменных, которые LMS
выдаёт при подключении PostgreSQL: `POSTGRES_CONNECTION_STRING`, `POSTGRES_DATABASE_NAME`,
`POSTGRES_HOST`, `POSTGRES_PORT` (схема приводится к `postgresql+asyncpg://`). Альтернатива —
явный `DATABASE_URL` (имеет приоритет). Отдельно задавать `DATABASE_URL` в LMS не нужно.

## Запуск локально

Требуется [uv](https://docs.astral.sh/uv/) и Python 3.13.

```bash
cp .env.example .env          # заполните CAPASHINO_API_TOKEN
uv sync
docker compose up db -d       # локальный PostgreSQL
uv run alembic upgrade head
uv run uvicorn src.main:app --reload --port 8000
```

Swagger: http://localhost:8000/docs

Или полностью в docker-compose (app + db):

```bash
CAPASHINO_API_TOKEN=<token> docker compose up --build
```

Локально без Kafka работают REST-эндпоинты и интеграции с Capashino; консьюмер/outbox
поднимаются автоматически при наличии брокера (lifespan приложения).

## Деплой (LMS / Kubernetes)

1. Соберите образ по [Dockerfile](Dockerfile) (CI `.github/workflows` билдит и пушит образ,
   LMS деплоит в namespace студента).
2. В разделе «Переменные окружения» сервиса укажите обязательные переменные из таблицы выше
   (см. также уже настроенные `POSTGRES_*`).
3. При старте пода `run.sh` выполняет `alembic upgrade head`, затем запускает
   `uvicorn src.main:app` на порту 8000.
4. Проверьте доступность: `GET /openapi.json` должен отвечать 200.

## Миграции

```bash
uv run alembic upgrade head    # применить
uv run alembic revision \
   --autogenerate -m "comment" # новая ревизия
```

Ревизии: базовая схема `orders` → `payment_callbacks` → `outbox_events`/`inbox_events` →
расширение колонок внешних ID до varchar(128/255) (защита от `value too long` на PostgreSQL).

## Проверка кода

```bash
uv run ruff check .
uv run ruff format --check .
```
