from abc import ABC, abstractmethod

from src.domain.entities import Order, PaymentCallback


class OrderRepository(ABC):
    """Абстрактный репозиторий для работы с заказами."""

    @abstractmethod
    async def get_by_id(self, order_id: str) -> Order | None:
        """Получить заказ по ID."""

    @abstractmethod
    async def get_by_idempotency_key(self, key: str) -> Order | None:
        """Получить заказ по ключу идемпотентности."""

    @abstractmethod
    async def create(self, order: Order) -> Order:
        """Создать новый заказ."""

    @abstractmethod
    async def update(self, order: Order) -> Order:
        """Сохранить изменения существующего заказа (статус, payment_id и т.п.)."""


class PaymentCallbackRepository(ABC):
    """Абстрактный репозиторий обработанных callback-ов Payments Service.

    Обеспечивает идемпотентность обработки callback-ов: факт обработки
    хранится в БД в рамках той же транзакции UoW, что и изменение заказа.
    """

    @abstractmethod
    async def exists(self, payment_id: str) -> bool:
        """Возвращает True, если callback с таким payment_id уже обработан."""

    @abstractmethod
    async def save(self, callback: PaymentCallback) -> None:
        """Записать факт обработки callback-а."""
