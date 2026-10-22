"""Outbox-воркер: надёжная доставка событий из таблицы Outbox в Kafka.

Цикл воркера (фоновая задача, живёт в lifespan приложения):

1. читает партию неотправленных записей `outbox_events`;
2. публикует каждую в Kafka (подтверждённая доставка);
3. помечает успешно отправленные `published = True` отдельной
   транзакцией.

Гарантия — at-least-once: если процесс упадёт после публикации, но до
пометки, событие будет отправлено повторно; потребители (Shipping
Service) идемпотентны по `idempotency_key`/ID события.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable

from src.application.ports.event_publisher import EventPublisher
from src.infrastructure.persistence.uow import UnitOfWorkImpl
from src.settings import Settings

logger = logging.getLogger(__name__)


class OutboxWorker:
    """Фоновый публикатор событий из Outbox."""

    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWorkImpl],
        publisher: EventPublisher,
        settings: Settings,
        *,
        batch_size: int = 50,
    ):
        self._uow_factory = uow_factory
        self._publisher = publisher
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
        """Один цикл: прочитать outbox и опубликовать pending-события.

        Возвращает число опубликованных событий. Пустой цикл — no-op.
        """
        uow = self._uow_factory()
        async with uow:
            records = await uow.outbox.fetch_pending(limit=self._batch_size)
            if not records:
                return 0
            published_ids: list[int] = []
            for record in records:
                try:
                    await self._publisher.publish_raw(
                        topic=record.topic,
                        key=record.aggregate_id,
                        payload=record.payload,
                    )
                except Exception:
                    logger.exception(
                        "Failed to publish outbox record %s (%s), will retry",
                        record.id,
                        record.event_type,
                    )
                    break
                published_ids.append(record.id)

            if published_ids:
                await uow.outbox.mark_published(published_ids)
                await uow.commit()
                logger.info("Published %d outbox event(s)", len(published_ids))
            return len(published_ids)
