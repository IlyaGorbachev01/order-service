"""Постановка уведомлений о статусах заказов в transactional outbox.

Гарантия доставки — **at-least-once** через тот же механизм, что и для
Kafka-события `order.paid`: use case пишет запись `OutboxEventType.NOTIFICATION`
в outbox **в той же транзакции**, что и смена статуса заказа (атомарно:
либо статус изменён и уведомление поставлено в очередь, либо откат обоих).
Outbox-воркер разбирает записи: `kafka_event` -> Kafka, `notification` ->
HTTP-клиент Notifications Service; при ошибке запись не помечается
`published` и ретраится в следующем цикле — доставка переживает любой
рестарт процесса (краш пода, rolling update, kill).

Повторная доставка безопасна: ключ идемпотентности детерминирован —
`{order_id}:{status}` (см. `domain.notifications`), поэтому Notifications
Service не создаёт дубликат уведомления.

Этот модуль заменяет прежний best-effort путь доставки (`OrderNotifier` +
`BackgroundTasks` в HTTP / `asyncio`-task в worker): уведомления больше
нигде не отправляются «из памяти» процесса — единый механизм доставки
(outbox) вместо двух. Побочно это убирает весь background-слой
(`OrderNotifier`, `NotificationDispatcher`, `InflightTracker`).
"""

import logging

from src.application.ports.repositories import OutboxRepository
from src.domain.entities import Order, OrderStatus
from src.domain.events import NotificationCommand, OutboxEventType

logger = logging.getLogger(__name__)


def build_notification_command(order: Order) -> NotificationCommand:
    """Собрать доменную команду уведомления из текущего состояния заказа.

    Причина отмены (`cancel_reason`) включается только для CANCELLED:
    текст уведомления генерируется на границе отправки из пары
    (status, reason) — один источник правды в домене.
    """
    reason = order.cancel_reason if order.status is OrderStatus.CANCELLED else None
    return NotificationCommand(
        order_id=order.id,
        user_id=order.user_id,
        status=order.status.value,
        reason=reason,
    )


async def enqueue_status_notification(
    outbox: OutboxRepository,
    order: Order,
) -> None:
    """Поставить уведомление о текущем статусе заказа в Outbox.

    Запись добавляется в ТЕКУЩУЮ транзакцию UoW — ту же, в которой
    выполняется `orders.update(order)` (или `orders.create(order)`), —
    поэтому уведомление не потеряется при сбое процесса и не будет
    поставлено при откате транзакции.
    """
    command = build_notification_command(order)
    await outbox.add_event(
        aggregate_id=order.id,
        event_type=OutboxEventType.NOTIFICATION.value,
        payload=command.to_payload(),
        topic="",  # not a Kafka record: delivered via HTTP by the outbox worker
    )
    logger.info(
        "Notification enqueued to outbox for order %s (status %s)",
        order.id,
        order.status.value,
    )
