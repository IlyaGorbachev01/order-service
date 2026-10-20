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


class InvalidOrderTransitionError(OrderError):
    """Запрещённый переход статуса заказа (нарушение машины состояний)."""


class DuplicatePaymentCallbackError(OrderError):
    """Callback с таким payment_id уже был обработан (идемпотентность)."""
