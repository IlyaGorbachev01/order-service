import json
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.ports.repositories import (
    InboxRepository,
    OrderRepository,
    OutboxRecord,
    OutboxRepository,
    PaymentCallbackRepository,
)
from src.domain.entities import Order, OrderStatus, PaymentCallback
from src.domain.events import ShipmentReceivedEvent
from src.domain.exceptions import (
    ConcurrentOrderCreationError,
    DuplicateInboxEventError,
    DuplicatePaymentCallbackError,
    OrderNotFoundError,
)
from src.infrastructure.persistence.models import (
    InboxEventModel,
    OrderModel,
    OutboxEventModel,
    PaymentCallbackModel,
)


def _is_unique_violation(error: IntegrityError) -> bool:
    """Проверяет, что IntegrityError вызван нарушением unique-ограничения.

    Работает для PostgreSQL (asyncpg/psycopg), SQLite и SQL Server, где код
    ошибки отличается; для остальных случаев используется общий признак
    SQLAlchemy — «duplicate key / UNIQUE constraint failed».
    """
    pgcode = getattr(error.orig, "pgcode", None) or getattr(
        error.orig, "sqlstate", None
    )
    if pgcode == "23505":  # PostgreSQL unique_violation
        return True
    message = str(error).lower()
    return (
        "unique" in message
        or "duplicate key" in message
        or "constraint failed" in message
    )


def _utcnow() -> datetime:
    return datetime.now(UTC)


class OrderRepositoryImpl(OrderRepository):
    """SQLAlchemy-реализация репозитория заказов.

    Работает с переданной асинхронной сессией; управление
    транзакцией (commit/rollback) остаётся за Unit of Work.
    Все методы выполняют flush (не commit), чтобы записи
    были видны в рамках текущей транзакции, но фиксация
    остаётся на стороне UoW.
    """

    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_by_id(self, order_id: str) -> Order | None:
        """Найти заказ по идентификатору или вернуть None."""
        stmt = select(OrderModel).where(OrderModel.id == order_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        return self._to_entity(model) if model else None

    async def get_by_idempotency_key(self, key: str) -> Order | None:
        """Найти заказ по ключу идемпотентности или вернуть None."""
        stmt = select(OrderModel).where(OrderModel.idempotency_key == key)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        return self._to_entity(model) if model else None

    async def create(self, order: Order) -> bool:
        """Добавить заказ в сессию и выполнить flush.

        Возвращает True, если заказ создан этим вызовом, и False, если
        заказ с таким `idempotency_key` уже существует (идемпотентный
        повтор).

        Гонка двух запросов с одинаковым `idempotency_key` приводит к
        нарушению уникального индекса. Такое состояние переводится в
        доменное `ConcurrentOrderCreationError`, чтобы use case смог
        вернуться к существующему заказу, а не отдать клиенту 500.

        Важно: после IntegrityError сессия становится «грязной» (все
        последующие операции падают с PendingRollbackError), поэтому её
        немедленно откатываем — иначе повторное чтение существующего
        заказа в той же транзакции давало бы 500 Internal Server Error.
        """
        model = OrderModel(
            id=order.id,
            user_id=order.user_id,
            item_id=order.item_id,
            quantity=order.quantity,
            status=order.status.value,
            idempotency_key=order.idempotency_key,
            payment_id=order.payment_id,
            cancel_reason=order.cancel_reason,
            created_at=order.created_at,
            updated_at=order.updated_at,
        )
        self._session.add(model)
        try:
            await self._session.flush()
        except IntegrityError as e:
            # Откатываем сессию, чтобы она снова стала пригодна для
            # работы (иначе UoW.rollback() тоже упадёт поверх ошибки).
            await self._session.rollback()
            if _is_unique_violation(e):
                raise ConcurrentOrderCreationError(
                    f"Заказ с idempotency_key={order.idempotency_key} "
                    f"уже создаётся конкурентным запросом."
                ) from e
            raise
        return True

    async def update(self, order: Order) -> Order:
        """Сохранить изменения существующего заказа (статус, payment_id и т.п.)."""
        model = await self._session.get(OrderModel, order.id)
        if model is None:
            raise OrderNotFoundError(f"Заказ {order.id} не найден для обновления.")
        model.status = order.status.value
        model.payment_id = order.payment_id
        model.cancel_reason = order.cancel_reason
        model.updated_at = order.updated_at
        await self._session.flush()
        return order

    @staticmethod
    def _to_entity(model: OrderModel) -> Order:
        """Преобразовать ORM-модель в доменную сущность."""
        order = Order(
            id=model.id,
            user_id=model.user_id,
            item_id=model.item_id,
            quantity=model.quantity,
            status=OrderStatus(model.status),
            idempotency_key=model.idempotency_key,
            created_at=model.created_at,
            updated_at=model.updated_at,
            payment_id=model.payment_id,
        )
        if model.cancel_reason:
            order.cancel_reason = model.cancel_reason
        return order


class PaymentCallbackRepositoryImpl(PaymentCallbackRepository):
    """SQLAlchemy-реализация репозитория обработанных callback-ов.

    Идемпотентность обеспечивается первичным ключом payment_id:
    повторная вставка бросает IntegrityError, который транслируется
    в доменный DuplicatePaymentCallbackError.
    """

    def __init__(self, session: AsyncSession):
        self._session = session

    async def exists(self, payment_id: str) -> bool:
        """Возвращает True, если callback с таким payment_id уже обработан."""
        stmt = select(PaymentCallbackModel.payment_id).where(
            PaymentCallbackModel.payment_id == payment_id
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def save(self, callback: PaymentCallback) -> None:
        """Записать факт обработки callback-а (flush без commit — за UoW)."""
        model = PaymentCallbackModel(
            payment_id=callback.payment_id,
            order_id=callback.order_id,
            status=callback.status.value,
            amount=callback.amount,
            error_message=callback.error_message,
            received_at=callback.received_at,
        )
        self._session.add(model)
        try:
            await self._session.flush()
        except IntegrityError as e:
            if _is_unique_violation(e):
                raise DuplicatePaymentCallbackError(
                    f"Callback для платежа {callback.payment_id} уже обработан."
                ) from e
            raise


class OutboxRepositoryImpl(OutboxRepository):
    """SQLAlchemy-реализация таблицы Outbox.

    События пишутся в ту же транзакцию UoW, что и изменение заказа
    (атомарность «данные + событие»). Воркер-публикатор выбирает
    неотправленные записи и помечает их отправленными после доставки
    в Kafka.
    """

    def __init__(self, session: AsyncSession):
        self._session = session

    async def add_event(
        self,
        *,
        aggregate_id: str,
        event_type: str,
        payload: dict,
        topic: str,
    ) -> None:
        """Поставить событие в очередь на публикацию (flush без commit)."""
        model = OutboxEventModel(
            topic=topic,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=json.dumps(payload, ensure_ascii=False),
            created_at=_utcnow(),
            published=False,
        )
        self._session.add(model)
        await self._session.flush()

    async def fetch_pending(self, limit: int = 50) -> Sequence[OutboxRecord]:
        """Выбрать неотправленные события по возрастанию id (FIFO)."""
        stmt = (
            select(OutboxEventModel)
            .where(OutboxEventModel.published.is_(False))
            .order_by(OutboxEventModel.id)
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [
            OutboxRecord(
                id=model.id,
                topic=model.topic,
                aggregate_id=model.aggregate_id,
                event_type=model.event_type,
                payload=model.payload_dict(),
            )
            for model in result.scalars().all()
        ]

    async def mark_published(self, record_ids: Sequence[int]) -> None:
        """Пометить события отправленными (после успешной доставки в Kafka)."""
        if not record_ids:
            return
        stmt = (
            update(OutboxEventModel)
            .where(OutboxEventModel.id.in_(record_ids))
            .values(published=True)
        )
        await self._session.execute(stmt)
        await self._session.flush()


class InboxRepositoryImpl(InboxRepository):
    """SQLAlchemy-реализация таблицы Inbox (дедупликация входящих событий).

    Идемпотентность обеспечивается уникальным индексом `event_id`:
    повторная вставка бросает IntegrityError, который транслируется в
    `DuplicateInboxEventError` — штатный сигнал «событие уже обработано».
    """

    def __init__(self, session: AsyncSession):
        self._session = session

    async def is_processed(self, event_id: str) -> bool:
        """Возвращает True, если событие с таким event_id уже обработано."""
        stmt = select(InboxEventModel.id).where(InboxEventModel.event_id == event_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def mark_processed(self, event: ShipmentReceivedEvent) -> None:
        """Записать факт обработки входящего события (flush без commit)."""
        model = InboxEventModel(
            event_id=event.event_id,
            event_type=event.event_type,
            order_id=event.order_id,
            payload=json.dumps(event.to_payload(), ensure_ascii=False),
            processed_at=_utcnow(),
        )
        self._session.add(model)
        try:
            await self._session.flush()
        except IntegrityError as e:
            if _is_unique_violation(e):
                raise DuplicateInboxEventError(
                    f"Событие {event.event_id} уже обработано (Inbox)."
                ) from e
            raise
