import logging

from src.application.ports.uow import BaseUnitOfWork
from src.application.usecases.notify_order_status import OrderNotifier
from src.domain.events import ShipmentReceivedEvent
from src.domain.exceptions import (
    DuplicateInboxEventError,
    InvalidOrderTransitionError,
    OrderNotFoundError,
)

logger = logging.getLogger(__name__)


class _DuplicateEventSignalError(Exception):
    """Служебный сигнал: событие уже обрабатывается конкурентной транзакцией.

    Пробрасывается из контекстного менеджера UoW, чтобы транзакция была
    откачена (никаких частичных изменений), и гасится в `__call__`:
    для консьюмера это штатная ситуация (no-op, можно коммитить offset).
    """


class HandleShipmentEventUseCase:
    """Use Case: обработка входящего события от Shipping Service (Inbox).

    - `order.shipped`   -> заказ переходит в SHIPPED;
    - `order.cancelled` -> заказ переходит в CANCELLED.

    Идемпотентность обеспечивается Inbox-паттерном: проверка `is_processed`
    и запись факта обработки (`mark_processed`) выполняются в одной
    транзакции с изменением заказа. Повторные доставки того же события
    (at-least-once семантика Kafka) и гонка между экземплярами консьюмера
    обрабатываются как no-op.
    """

    def __init__(
        self,
        uow: BaseUnitOfWork,
        notifier: OrderNotifier | None = None,
    ):
        self._uow = uow
        # Общий синглтонный нотификатор
        self._notifier = notifier if notifier is not None else OrderNotifier()

    async def __call__(self, event: ShipmentReceivedEvent) -> None:
        """Обработать событие доставки.

        Бросает `OrderNotFoundError`, если заказ не найден (консьюмер должен
        заалертиться/записать ошибку, но не валить процесс). Все прочие
        ситуации (дубликаты, недопустимые переходы) — no-op.
        """
        try:
            await self._process(event)
        except _DuplicateEventSignalError:
            logger.info(
                "Shipment event %s concurrently processed by another replica",
                event.event_id,
            )

    async def _process(self, event: ShipmentReceivedEvent) -> None:
        if not (event.is_shipped or event.is_cancelled):
            logger.warning(
                "Ignoring shipment event with unknown type: %s",
                event.event_type,
            )
            return

        async with self._uow() as uow:
            # 1. Inbox: событие уже обработано?
            if await uow.inbox.is_processed(event.event_id):
                logger.debug(
                    "Duplicate shipment event ignored: event_id=%s",
                    event.event_id,
                )
                return

            # 2. Найти заказ
            order = await uow.orders.get_by_id(event.order_id)
            if order is None:
                raise OrderNotFoundError(
                    f"Заказ {event.order_id} не найден "
                    f"(shipment event_id={event.event_id})."
                )

            # 3. Применить доменную машину состояний
            try:
                if event.is_shipped:
                    order.mark_shipped()
                else:
                    order.mark_cancelled(reason=event.reason)
            except InvalidOrderTransitionError:
                # Заказ уже в терминальном/несовместимом статусе — фиксируем
                # факт обработки события, чтобы не зациклить ретраи консьюмера.
                logger.warning(
                    "Shipment event %s for order %s: transition from %s "
                    "is not allowed, marking event as processed.",
                    event.event_id,
                    order.id,
                    order.status,
                )
                await uow.inbox.mark_processed(event)
                return

            # 4. Атомарно: заказ + факт обработки события (Inbox)
            await uow.orders.update(order)
            try:
                await uow.inbox.mark_processed(event)
            except DuplicateInboxEventError:
                logger.info(
                    "Concurrent duplicate shipment event: event_id=%s",
                    event.event_id,
                )
                raise _DuplicateEventSignalError from None

            logger.info(
                "Order %s status updated to %s by shipment event %s (%s)",
                order.id,
                order.status.value,
                event.event_id,
                event.event_type,
            )

        # Уведомление о новом статусе (SHIPPED/CANCELLED) — после
        # коммита транзакции. Ошибки Notifications Service логируются
        # внутри OrderNotifier и не прерывают обработку события консьюмером.
        self._notifier.notify(order)
