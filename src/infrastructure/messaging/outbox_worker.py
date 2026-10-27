"""Outbox-воркер: надёжная доставка записей из таблицы Outbox.

Единый механизм доставки всех побочных эффектов (at-least-once): воркер
разбирает захваченные записи по назначению (`OutboxEventType`):

- ``kafka_event``  — публикация в Kafka-топик из записи (событие
  ``order.paid`` для Shipping Service);
- ``notification`` — HTTP-доставка через клиент Notifications Service:
  payload разбирается в :class:`NotificationCommand`, текст сообщения и
  детерминированный ключ идемпотентности ``{order_id}:{status}``
  формируются доменными функциями (см. ``domain.notifications``).

Цикл воркера:

1. *захватывает* партию неотправленных записей `outbox_events` транзакцией
   с `FOR UPDATE SKIP LOCKED` (PostgreSQL) — одна запись не обрабатывается
   двумя экземплярами воркера одновременно;
2. доставляет каждую по её типу (Kafka / HTTP Notifications);
3. помечает успешно отправленные `published = True` в той же транзакции
   захвата и завершает её.

Гарантия — at-least-once: если процесс упадёт после публикации, но до
коммита пометки (или при ошибке доставки запись вообще не помечается),
блокировка снимется и событие будет доставлено повторно; потребители
идемпотентны: Shipping — по `idempotency_key`/ID события, Notifications —
по детерминированному ключу `{order_id}:{status}`. Ошибки Notifications
Service не ломают основной поток: use case ничего не отправляет сам, а
ретраи происходят в цикле воркера.

Воркер запускается двумя способами (см. `src/bin/worker.py`):

- **отдельным процессом** (`python -m src.bin.worker`, в деплое — контейнер
  из `run-worker.sh`) — канонический способ по требованию ТЗ: изоляция от
  HTTP-нагрузки API и возможность масштабировать реплики; лишние копии
  безопасны благодаря claim-транзакции со SKIP LOCKED;
- фоновой задачей внутри процесса API это больше **не** делается: lifespan
  HTTP-сервиса воркер и консьюмер не поднимает.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable

from src.application.ports.event_publisher import EventPublisher
from src.application.ports.notifications import (
    NotificationClient,
    NotificationRequest,
)
from src.application.ports.repositories import OutboxRecord
from src.domain.entities import OrderStatus
from src.domain.events import NotificationCommand, OutboxEventType
from src.domain.notifications import (
    normalize_user_id,
    notification_idempotency_key,
    notification_message,
)
from src.infrastructure.persistence.uow import UnitOfWorkImpl
from src.settings import Settings

logger = logging.getLogger(__name__)


class OutboxWorker:
    """Публикатор событий из Outbox (безопасен для нескольких экземпляров).

    Назначение записи определяет способ доставки (`OutboxEventType`):
    ``kafka_event`` -> Kafka-продюсер, ``notification`` -> HTTP-клиент
    Notifications Service. Оба пути — at-least-once: при ошибке запись
    не помечается доставленной и будет повторена в следующем цикле.
    """

    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWorkImpl],
        publisher: EventPublisher,
        settings: Settings,
        *,
        notifications_client: NotificationClient | None = None,
        batch_size: int = 50,
    ):
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._notifications_client = notifications_client
        self._poll_interval = settings.outbox_poll_interval
        self._batch_size = batch_size
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """Запустить фоновую задачу (идемпотентно)."""
        if self._task is not None:
            return
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run(), name="outbox-worker")
        logger.info(
            "Outbox worker started (poll_interval=%.1fs, batch=%d)",
            self._poll_interval,
            self._batch_size,
        )

    async def stop(self) -> None:
        """Остановить воркер и дождаться завершения текущей итерации."""
        if self._task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._task, timeout=10)
        except TimeoutError:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        logger.info("Outbox worker stopped")

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.publish_pending_once()
            except Exception:
                logger.exception("Outbox publish cycle failed, retrying")
            # ждём между циклами, но реагируем на stop() без задержки
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._poll_interval,
                )

    async def publish_pending_once(self) -> int:
        """Один цикл: захватить партию outbox-записей и доставить их.

        Возвращает число доставленных записей. Пустой цикл — no-op.

        Транзакция захвата (`claim_pending` → FOR UPDATE SKIP LOCKED на
        PostgreSQL) остаётся открытой на время доставки и закрывается
        коммитом пометки `published = True`. Это делает воркера пригодным
        к горизонтальному масштабированию: несколько экземпляров не
        обрабатывают одну запись одновременно. При сбое до коммита
        блокировка снимается и запись повторяется (at-least-once).
        """
        uow = self._uow_factory()
        async with uow:
            records = await uow.outbox.claim_pending(limit=self._batch_size)
            if not records:
                return 0
            published_ids: list[int] = []
            for record in records:
                try:
                    await self._deliver(record)
                except Exception:
                    logger.exception(
                        "Failed to deliver outbox record %s (%s), will retry",
                        record.id,
                        record.event_type,
                    )
                    break
                published_ids.append(record.id)

            if published_ids:
                await uow.outbox.mark_published(published_ids)
                await uow.commit()
                logger.info("Delivered %d outbox event(s)", len(published_ids))
            return len(published_ids)

    async def _deliver(self, record: OutboxRecord) -> None:
        """Доставить одну запись outbox в зависимости от её типа.

        - ``notification`` -> HTTP-клиент Notifications Service;
        - всё остальное (``kafka_event``) -> Kafka-топик из записи.

        Любая ошибка доставки пробрасывается наружу: вызывающий цикл не
        помечает запись доставленной, и она будет повторена позже.
        """
        if record.event_type == OutboxEventType.NOTIFICATION:
            await self._deliver_notification(record)
            return
        await self._publisher.publish_raw(
            topic=record.topic,
            key=record.aggregate_id,
            payload=record.payload,
        )

    async def _deliver_notification(self, record: OutboxRecord) -> None:
        """Отправить уведомление из payload'а команды NotificationCommand.

        Текст сообщения и ключ идемпотентности `{order_id}:{status}`
        формируются доменными функциями — один источник правды; повторная
        доставка тем же ключом не создаёт дубликат на стороне сервиса.
        """
        if self._notifications_client is None:
            raise RuntimeError(
                "Outbox worker received a NOTIFICATION record but no "
                "notifications client was configured."
            )
        command = NotificationCommand.from_payload(record.payload)
        try:
            status = OrderStatus(command.status)
        except ValueError as e:
            # Повреждённый/неизвестный статус в payload — такие записи
            # пишет только наш же use case, поэтому это аномалия данных:
            # пробрасываем ошибку наружу, запись не помечается доставленной.
            raise ValueError(
                f"Unknown order status in notification payload: {command.status!r}"
            ) from e
        request = NotificationRequest(
            message=notification_message(status, reason=command.reason),
            user_id=normalize_user_id(command.user_id),
            reference_id=command.order_id,
            idempotency_key=notification_idempotency_key(command.order_id, status),
        )
        await self._notifications_client.send_notification(request)
