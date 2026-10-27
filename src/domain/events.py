from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

ORDER_PAID_EVENT = "order.paid"
ORDER_SHIPPED_EVENT = "order.shipped"
ORDER_CANCELLED_EVENT = "order.cancelled"


class ShipmentEventType(StrEnum):
    """Типы событий из топика Shipping Service (`student_system-shipment.events`)."""

    SHIPPED = ORDER_SHIPPED_EVENT
    CANCELLED = ORDER_CANCELLED_EVENT


@dataclass(frozen=True)
class DomainEvent:
    """Базовое доменное событие.

    Наследники — неизменяемые (frozen) value objects: событие фиксирует факт,
    который уже произошёл в домене, и не должен мутировать после создания.
    """

    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))


# --- Типы записей таблицы Outbox ---------------------------------------------
# Outbox — единый надёжный канал побочных эффектов (at-least-once): запись с
# Kafka-топиком публикуется в брокер, запись NOTIFICATION доставляется HTTP-
# клиентом Notifications Service. Воркер разбирает партии по этому признаку.


class OutboxEventType(StrEnum):
    """Назначение записи outbox: куда и как её доставлять."""

    KAFKA_EVENT = "kafka_event"
    NOTIFICATION = "notification"


@dataclass(frozen=True)
class NotificationCommand(DomainEvent):
    """Команда доставки уведомления о статусе заказа (через Outbox).

    Доменный value object: фиксирует факт «при переходе заказа в `status`
    пользователя нужно уведомить». Пишется в outbox В ТОЙ ЖЕ транзакции,
    что и смена статуса, поэтому уведомление переживает любой рестарт
    процесса (at-least-once). Повторная доставка безопасна: ключ
    идемпотентности детерминирован — `{order_id}:{status}` (см.
    `domain.notifications.notification_idempotency_key`).

    Текст сообщения НЕ хранится в команде: он генерируется на границе
    отправки из `status` + `reason` (домен), чтобы не дублировать один
    источник правды в двух местах.
    """

    order_id: str = ""
    user_id: str = ""
    status: str = ""
    reason: str | None = None

    def to_payload(self) -> dict:
        """Сериализация команды в JSON payload записи outbox."""
        return {
            "event_type": OutboxEventType.NOTIFICATION.value,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "status": self.status,
            "reason": self.reason,
        }

    @classmethod
    def from_payload(cls, payload: dict) -> "NotificationCommand":
        """Восстановить команду из JSON payload'а записи outbox."""
        return cls(
            order_id=str(payload.get("order_id", "")),
            user_id=str(payload.get("user_id", "")),
            status=str(payload.get("status", "")),
            reason=payload.get("reason"),
        )


@dataclass(frozen=True)
class OrderPaidEvent(DomainEvent):
    """Событие `order.paid`: заказ оплачен, готов к доставке.

    Публикуется Order Service в топик `student_system-order.events`
    (через Outbox) после успешного платежа.
    """

    event_type: str = ORDER_PAID_EVENT
    order_id: str = ""
    item_id: str = ""
    quantity: int = 0
    idempotency_key: str = ""

    def to_dict(self) -> dict:
        """Сериализация в формат payload'а Kafka-события (по контракту задания)."""
        return {
            "event_type": self.event_type,
            "order_id": self.order_id,
            "item_id": self.item_id,
            "quantity": self.quantity,
            "idempotency_key": self.idempotency_key,
        }


@dataclass(frozen=True)
class ShipmentReceivedEvent(DomainEvent):
    """Входящее событие от Shipping Service (топик `student_system-shipment.events`).

    Объединяет форматы `order.shipped` (есть `shipment_id`) и `order.cancelled`
    (есть `reason`): оба поля опциональны, смысл определяет `event_type`.
    Идемпотентность обработки обеспечивается Inbox-паттерном по ключу
    `event_id` (уникальный идентификатор конкретного события).
    """

    event_id: str = ""
    event_type: str = ""
    order_id: str = ""
    item_id: str = ""
    quantity: int = 0
    shipment_id: str | None = None
    reason: str | None = None

    @property
    def is_shipped(self) -> bool:
        """Событие означает успешную отправку заказа в доставку."""
        return self.event_type == ShipmentEventType.SHIPPED

    @property
    def is_cancelled(self) -> bool:
        """Событие означает отмену доставки (заказ должен быть отменён)."""
        return self.event_type == ShipmentEventType.CANCELLED

    def to_payload(self) -> dict:
        """Сериализация события обратно в JSON-формат топика (для Inbox)."""
        payload = {
            "event_type": self.event_type,
            "order_id": self.order_id,
            "item_id": self.item_id,
            "quantity": self.quantity,
        }
        if self.shipment_id is not None:
            payload["shipment_id"] = self.shipment_id
        if self.reason is not None:
            payload["reason"] = self.reason
        return payload

    @classmethod
    def from_payload(
        cls,
        payload: dict,
        *,
        fallback_event_id: str = "",
    ) -> "ShipmentReceivedEvent":
        """Построить событие из JSON-словаря, полученного из Kafka.

        Неизвестные поля игнорируются; отсутствующие строки становятся `None`.
        `fallback_event_id` используется, если в событии нет собственного
        идентификатора (например `kafka-<topic>-<partition>-<offset>`).
        """
        event_id = str(
            payload.get("event_id") or payload.get("shipment_id") or fallback_event_id
        )
        quantity = payload.get("quantity") or 0
        return cls(
            event_id=event_id,
            event_type=str(payload.get("event_type", "")),
            order_id=str(payload.get("order_id", "")),
            item_id=str(payload.get("item_id", "")),
            quantity=int(quantity),
            shipment_id=payload.get("shipment_id"),
            reason=payload.get("reason"),
        )
