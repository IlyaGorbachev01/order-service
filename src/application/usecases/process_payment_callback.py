import logging

from src.application.ports.repositories import OutboxRepository
from src.application.ports.uow import BaseUnitOfWork
from src.application.usecases.create_order import NotificationPort
from src.domain.entities import PaymentCallback
from src.domain.events import ORDER_PAID_EVENT, OrderPaidEvent
from src.domain.exceptions import (
    DuplicatePaymentCallbackError,
    InvalidOrderTransitionError,
    OrderNotFoundError,
)

logger = logging.getLogger(__name__)


class _DuplicateCallbackSignalError(Exception):
    """Служебный сигнал: callback уже обрабатывается конкурентной транзакцией.

    Пробрасывается наружу из контекстного менеджера UoW, чтобы транзакция
    была откачена (никаких частичных изменений/дублей события), и тут же
    «гасится» в `__call__`: для HTTP-слоя это штатная ситуация (no-op).
    """


def build_order_paid_payload(
    order_id: str,
    item_id: str,
    quantity: int,
    idempotency_key: str,
) -> dict:
    """Собрать payload события `order.paid` строго по контракту задания."""
    return OrderPaidEvent(
        order_id=order_id,
        item_id=item_id,
        quantity=quantity,
        idempotency_key=idempotency_key,
    ).to_dict()


async def enqueue_order_paid_event(
    outbox: OutboxRepository,
    *,
    topic: str,
    order_id: str,
    item_id: str,
    quantity: int,
    idempotency_key: str,
) -> None:
    """Поставить событие `order.paid` в Outbox (в текущей транзакции UoW).

    Запись происходит в той же транзакции, что и перевод заказа в PAID,
    поэтому событие не потеряется при сбое Kafka и не продублируется при
    откате транзакции. Дедупликация обеспечивается идемпотентностью самого
    callback-а (таблица payment_callbacks): повторный callback не дойдёт
    до этого места.
    """
    await outbox.add_event(
        aggregate_id=order_id,
        event_type=ORDER_PAID_EVENT,
        payload=build_order_paid_payload(
            order_id=order_id,
            item_id=item_id,
            quantity=quantity,
            idempotency_key=idempotency_key,
        ),
        topic=topic,
    )
    logger.info("Event order.paid enqueued to outbox for order %s", order_id)


class ProcessPaymentCallbackUseCase:
    """Use Case: обработка callback-а от Payments Service.

    Идемпотентность: повторные callback-и с тем же `payment_id`
    игнорируются (факт обработки хранится в таблице payment_callbacks
    в той же транзакции UoW, что и изменение заказа).

    - status == succeeded -> заказ переходит в PAID;
    - status == failed    -> заказ переходит в CANCELLED.
    """

    def __init__(
        self,
        uow: BaseUnitOfWork,
        notifier: NotificationPort,
        *,
        order_events_topic: str = "",
    ):
        self._uow = uow
        self._order_events_topic = order_events_topic
        # Нотификатор передаётся СТРОГО через конструктор (DI): use case не
        # создаёт инфраструктурные реализации сам и ничего о них не знает.
        self._notifier = notifier

    async def __call__(self, callback: PaymentCallback) -> None:
        """Обработать callback платежа.

        Бросает `OrderNotFoundError`, если заказ не найден.
        Повторные/конкурентные дубликаты обрабатываются молча (no-op),
        чтобы endpoint всегда отвечал 200 OK сервису-источнику.
        """
        try:
            await self._process(callback)
        except _DuplicateCallbackSignalError:
            # Конкурентный экземпляр уже зафиксировал этот callback — no-op.
            return

    async def _process(self, callback: PaymentCallback) -> None:
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

            # 4. Outbox: атомарно с переходом статуса ставим событие order.paid
            #    на публикацию в Kafka (топик Shipping-интеграции).
            if callback.succeeded and self._order_events_topic:
                await enqueue_order_paid_event(
                    uow.outbox,
                    topic=self._order_events_topic,
                    order_id=order.id,
                    item_id=order.item_id,
                    quantity=order.quantity,
                    idempotency_key=order.idempotency_key or callback.payment_id,
                )

            # 5. Атомарно сохранить заказ и факт обработки callback-а
            await uow.orders.update(order)
            try:
                await uow.payment_callbacks.save(callback)
            except DuplicatePaymentCallbackError:
                # Гонка: параллельный экземпляр уже зафиксировал этот callback.
                # Откатываем свою транзакцию (UoW сделает rollback при исключении
                # либо коммит пустого дубля не нужен) — повтор обработает
                # следующий callback или retry источника.
                logger.info(
                    "Concurrent duplicate payment callback: payment_id=%s",
                    callback.payment_id,
                )
                raise _DuplicateCallbackSignalError from None

            logger.info(
                "Order %s status updated to %s by payment callback %s",
                order.id,
                order.status.value,
                callback.payment_id,
            )

        # Уведомление о новом статусе (PAID/CANCELLED) — после коммита
        # транзакции, чтобы не отправлять уведомление при последующем откате.
        # OrderNotifier перехватывает ошибки сервиса и лишь логирует их.
        await self._notifier.notify(order)
