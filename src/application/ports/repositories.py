from abc import ABC, abstractmethod

from src.domain.entities import Order


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
