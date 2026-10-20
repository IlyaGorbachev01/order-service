from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from src.domain.exceptions import InvalidOrderTransitionError


class OrderStatus(StrEnum):
    """Статусы заказа."""

    NEW = "NEW"
    PAID = "PAID"
    SHIPPED = "SHIPPED"
    CANCELLED = "CANCELLED"


# Разрешённые переходы состояний заказа
ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.NEW: frozenset({OrderStatus.PAID, OrderStatus.CANCELLED}),
    OrderStatus.PAID: frozenset({OrderStatus.SHIPPED, OrderStatus.CANCELLED}),
    OrderStatus.SHIPPED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
}


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass
class Order:
    """Бизнес-сущность заказа."""

    id: str
    user_id: str
    item_id: str
    quantity: int
    status: OrderStatus
    idempotency_key: str
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)
    payment_id: str | None = None

    def can_be_cancelled(self) -> bool:
        """Бизнес-правило: отменить можно только NEW или PAID."""
        return self.status in (OrderStatus.NEW, OrderStatus.PAID)

    def transition_to(self, new_status: OrderStatus) -> None:
        """Перевести заказ в новое состояние согласно машине состояний.

        Бросает InvalidOrderTransitionError, если переход запрещён. Идемпотентные
        повторные callback-и обрабатываются на уровне use case: здесь только
        инвариант домена.
        """
        if new_status not in ALLOWED_TRANSITIONS[self.status]:
            raise InvalidOrderTransitionError(
                f"Заказ {self.id}: переход {self.status.value} -> "
                f"{new_status.value} запрещён."
            )
        self.status = new_status
        self.updated_at = _utcnow()

    def mark_paid(self, payment_id: str | None = None) -> None:
        """Платёж успешен: NEW -> PAID."""
        self.transition_to(OrderStatus.PAID)
        if payment_id is not None:
            self.payment_id = payment_id

    def mark_cancelled(self, reason: str | None = None) -> None:
        """Отмена заказа (неуспешный платёж, нехватка стока и т.п.)."""
        self.transition_to(OrderStatus.CANCELLED)
        if reason:
            self.cancel_reason = reason

    @property
    def cancel_reason(self) -> str | None:
        """Причина отмены заказа (если заказ был отменён с указанием причины)."""
        return getattr(self, "_cancel_reason", None)

    @cancel_reason.setter
    def cancel_reason(self, value: str) -> None:
        self._cancel_reason = value


@dataclass(frozen=True)
class PaymentCallback:
    """Доменная сущность: результат обработки платежа Payments Service.

    Используется как для приёма callback-запроса, так и для записи факта
    его обработки (идемпотентность по payment_id).
    """

    payment_id: str
    order_id: str
    status: str  # "succeeded" | "failed"
    amount: str
    error_message: str | None = None
    received_at: datetime = field(default_factory=_utcnow)

    @property
    def succeeded(self) -> bool:
        """Успешен ли платёж по данным callback-а."""
        return self.status == "succeeded"
