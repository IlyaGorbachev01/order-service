import json
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Базовый класс декларативных моделей SQLAlchemy."""


class OrderModel(Base):
    """ORM-модель заказа."""

    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(36), nullable=False)
    item_id: Mapped[str] = mapped_column(String(36), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(
        String(64), unique=True, nullable=False, index=True
    )
    payment_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    cancel_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class PaymentCallbackModel(Base):
    """ORM-модель факта обработки callback-а Payments Service.

    Уникальный индекс по payment_id обеспечивает идемпотентность:
    повторный callback не пройдёт вставку (IntegrityError) и будет
    корректно обработан как дубликат.
    """

    __tablename__ = "payment_callbacks"

    payment_id: Mapped[str] = mapped_column(
        String(36), primary_key=True, autoincrement=False
    )
    order_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    amount: Mapped[str] = mapped_column(String(32), nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class OutboxEventModel(Base):
    """ORM-модель таблицы Outbox (исходящие события в Kafka).

    Событие пишется в ту же транзакцию, что и изменение заказа, — так
    гарантируется атомарность «данные + событие» (Outbox-паттерн).
    Отдельный воркер выбирает записи с `published = FALSE` и публикует их
    в брокер, после чего помечает отправленными.
    """

    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    def payload_dict(self) -> dict:
        """Десериализовать JSON-строку payload'а в словарь."""
        return json.loads(self.payload)


class InboxEventModel(Base):
    """ORM-модель таблицы Inbox (обработанные входящие события из Kafka).

    Уникальный индекс по `event_id` обеспечивает идемпотентность: повторная
    доставка того же события (at-least-once семантика Kafka) не пройдёт
    вставку (IntegrityError) и будет корректно пропущена консьюмером.
    """

    __tablename__ = "inbox_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(
        String(128), unique=True, nullable=False, index=True
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    order_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
