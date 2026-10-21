from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.ports.repositories import (
    OrderRepository,
    PaymentCallbackRepository,
)
from src.domain.entities import Order, OrderStatus, PaymentCallback
from src.domain.exceptions import (
    ConcurrentOrderCreationError,
    DuplicatePaymentCallbackError,
    OrderNotFoundError,
)
from src.infrastructure.persistence.models import OrderModel, PaymentCallbackModel


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

    async def create(self, order: Order) -> Order:
        """Добавить заказ в сессию и выполнить flush.

        Гонка двух запросов с одинаковым `idempotency_key` приводит к
        нарушению уникального индекса. Такое состояние переводится в
        доменное `ConcurrentOrderCreationError`, чтобы use case смог
        вернуться к существующему заказу, а не отдать клиенту 500.
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
            if _is_unique_violation(e):
                raise ConcurrentOrderCreationError(
                    f"Заказ с idempotency_key={order.idempotency_key} "
                    f"уже создаётся конкурентным запросом."
                ) from e
            raise
        return order

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
