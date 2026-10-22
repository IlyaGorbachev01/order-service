class OrderError(Exception):
    """Базовое исключение для ошибок заказа."""


class OrderNotFoundError(OrderError):
    """Заказ не найден."""


class InsufficientStockError(OrderError):
    """Недостаточно товара на складе."""


class DuplicateOrderError(OrderError):
    """Заказ с таким idempotency_key уже существует."""


class CatalogUnavailableError(OrderError):
    """Товар не найден в каталоге или Catalog Service недоступен."""


class PaymentServiceError(OrderError):
    """Payments Service недоступен или отклонил создание платежа."""


class NotificationServiceError(OrderError):
    """Notifications Service недоступен или вернул ошибку HTTP.

    Не блокирует основной процесс: use cases перехватывают это исключение
    и лишь логируют его.
    """


class InvalidOrderTransitionError(OrderError):
    """Запрещённый переход статуса заказа (нарушение машины состояний)."""


class DuplicatePaymentCallbackError(OrderError):
    """Callback с таким payment_id уже был обработан (идемпотентность)."""


class DuplicateInboxEventError(Exception):
    """Входящее событие с таким event_id уже обработано (Inbox-паттерн).

    Не является ошибкой заказа: сигнал штатной дедупликации повторной
    доставки события из Kafka (at-least-once семантика брокера).
    """


class ConcurrentOrderCreationError(OrderError):
    """Конкурентная попытка создать заказ с тем же idempotency_key.

    Возникает, когда несколько запросов (или несколько реплик сервиса)
    одновременно проходят проверку «заказа ещё нет» и затем сталкиваются
    на уникальном индексе `orders.idempotency_key`. Отрабатывается как
    штатная ситуация: use case повторяет чтение и возвращает уже
    существующий заказ (идемпотентность).
    """
