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
