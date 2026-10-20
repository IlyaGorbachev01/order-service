import logging

from src.application.ports.uow import BaseUnitOfWork
from src.domain.entities import PaymentCallback
from src.domain.exceptions import (
    InvalidOrderTransitionError,
    OrderNotFoundError,
)

logger = logging.getLogger(__name__)


class ProcessPaymentCallbackUseCase:
    """Use Case: обработка callback-а от Payments Service.

    Идемпотентность: повторные callback-и с тем же `payment_id`
    игнорируются (факт обработки хранится в таблице payment_callbacks
    в той же транзакции UoW, что и изменение заказа).

    - status == succeeded -> заказ переходит в PAID;
    - status == failed    -> заказ переходит в CANCELLED.
    """

    def __init__(self, uow: BaseUnitOfWork):
        self._uow = uow

    async def __call__(self, callback: PaymentCallback) -> None:
        """Обработать callback платежа.

        Бросает `OrderNotFoundError`, если заказ не найден.
        Повторные/конкурентные дубликаты обрабатываются молча (no-op),
        чтобы endpoint всегда отвечал 200 OK сервису-источнику.
        """
        async with self._uow() as uow:
            # 1. Идемпотентность: этот payment_id уже обработан?
            if await uow.payment_callbacks.exists(callback.payment_id):
                logger.info(
                    "Duplicate payment callback ignored: payment_id=%s",
                    callback.payment_id,
                )
                return

            # 2. Найти заказ
            order = await uow.orders.get_by_id(callback.order_id)
            if order is None:
                raise OrderNotFoundError(
                    f"Заказ {callback.order_id} не найден "
                    f"(callback payment_id={callback.payment_id})."
                )

            # 3. Применить доменную машину состояний
            try:
                if callback.succeeded:
                    order.mark_paid(payment_id=callback.payment_id)
                else:
                    order.mark_cancelled(reason=callback.error_message)
            except InvalidOrderTransitionError:
                # Заказ уже в терминальном статусе (например, отменён ранее) —
                # фиксируем факт обработки и выходим, чтобы не зациклить ретраи.
                logger.warning(
                    "Callback %s for order %s: transition from %s is not "
                    "allowed, marking callback as processed.",
                    callback.payment_id,
                    order.id,
                    order.status,
                )
                await uow.payment_callbacks.save(callback)
                return

            # 4. Атомарно сохранить заказ и факт обработки callback-а
            await uow.orders.update(order)
            await uow.payment_callbacks.save(callback)

            logger.info(
                "Order %s status updated to %s by payment callback %s",
                order.id,
                order.status.value,
                callback.payment_id,
            )
