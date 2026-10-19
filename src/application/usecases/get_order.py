from src.application.ports.uow import BaseUnitOfWork
from src.domain.entities import Order
from src.domain.exceptions import OrderNotFoundError


class GetOrderUseCase:
    """Use Case: получение заказа по идентификатору."""

    def __init__(self, uow: BaseUnitOfWork):
        self._uow = uow

    async def __call__(self, order_id: str) -> Order:
        """Получить заказ по ID."""
        async with self._uow() as uow:
            order = await uow.orders.get_by_id(order_id)
            if not order:
                raise OrderNotFoundError(f"Заказ {order_id} не найден")
            return order
