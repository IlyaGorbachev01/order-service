from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.ports.repositories import OrderRepository
from src.domain.entities import Order, OrderStatus
from src.infrastructure.persistence.models import OrderModel


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
        """Добавить заказ в сессию и выполнить flush."""
        model = OrderModel(
            id=order.id,
            user_id=order.user_id,
            item_id=order.item_id,
            quantity=order.quantity,
            status=order.status.value,
            idempotency_key=order.idempotency_key,
            created_at=order.created_at,
            updated_at=order.updated_at,
        )
        self._session.add(model)
        await self._session.flush()
        return order

    @staticmethod
    def _to_entity(model: OrderModel) -> Order:
        """Преобразовать ORM-модель в доменную сущность."""
        return Order(
            id=model.id,
            user_id=model.user_id,
            item_id=model.item_id,
            quantity=model.quantity,
            status=OrderStatus(model.status),
            idempotency_key=model.idempotency_key,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )
