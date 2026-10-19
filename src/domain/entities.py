from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class OrderStatus(StrEnum):
    """Статусы заказа."""

    NEW = "NEW"
    PAID = "PAID"
    SHIPPED = "SHIPPED"
    CANCELLED = "CANCELLED"


@dataclass
class Order:
    """Бизнес-сущность заказа."""

    id: str
    user_id: str
    item_id: str
    quantity: int
    status: OrderStatus
    idempotency_key: str
    created_at: datetime
    updated_at: datetime

    def can_be_cancelled(self) -> bool:
        """Бизнес-правило: отменить можно только NEW или PAID."""
        return self.status in (OrderStatus.NEW, OrderStatus.PAID)
