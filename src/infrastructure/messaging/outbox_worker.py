"""Outbox-воркер: надёжная доставка событий из таблицы Outbox в Kafka.

Цикл воркера:

1. *захватывает* партию неотправленных записей `outbox_events` транзакцией
   с `FOR UPDATE SKIP LOCKED` (PostgreSQL) — одна запись не обрабатывается
   двумя экземплярами воркера одновременно;
2. публикует каждую в Kafka (подтверждённая доставка, `acks=all`);
3. помечает успешно отправленные `published = True` в той же транзакции
   захвата и завершает её.

Гарантия — at-least-once: если процесс упадёт после публикации, но до
коммита пометки, блокировка снимется и событие будет опубликовано
повторно; потребители (Shipping Service) идемпотентны по
`idempotency_key`/ID события.

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
from src.infrastructure.persistence.uow import UnitOfWorkImpl
from src.settings import Settings

logger = logging.getLogger(__name__)


class OutboxWorker:
    """Публикатор событий из Outbox (безопасен для нескольких экземпляров)."""

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
        """Один цикл: захватить партию outbox-событий и опубликовать их.

        Возвращает число опубликованных событий. Пустой цикл — no-op.

        Транзакция захвата (`claim_pending` → FOR UPDATE SKIP LOCKED на
        PostgreSQL) остаётся открытой на время публикации и закрывается
        коммитом пометки `published = True`. Это делает воркера пригодным
        к горизонтальному масштабированию: несколько экземпляров (копии
        Deployment'а API или отдельные процессы `src/bin/outbox_worker.py`)
        не обрабатывают одну запись одновременно. При сбое до коммита
        блокировка снимается и событие повторяется (at-least-once).
        """
        uow = self._uow_factory()
        async with uow:
            records = await uow.outbox.claim_pending(limit=self._batch_size)
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
