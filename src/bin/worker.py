"""Точка запуска фоновых компонентов отдельным процессом (worker).

Состав процесса:
* :class:`KafkaEventPublisher` — продюсер топика `order.events`;
* :class:`OutboxWorker` — relay таблицы `outbox_events` в Kafka
  (партии захватываются транзакцией `FOR UPDATE SKIP LOCKED`, поэтому
  несколько копий воркера безопасны);
* :class:`ShipmentConsumer` — консьюмер топика `shipment.events` с
  Inbox-идемпотентностью на уровне use case.

Graceful shutdown: SIGTERM/SIGINT останавливает оба компонента и закрывает
ресурсы (продюсер, HTTP-клиенты, движок БД).
"""

import asyncio
import contextlib
import logging
import signal

from src.application.usecases.handle_shipment_event import HandleShipmentEventUseCase
from src.application.usecases.notification_dispatcher import NotificationDispatcher
from src.domain.events import ShipmentReceivedEvent
from src.infrastructure.http.notifications import NotificationsClientImpl
from src.infrastructure.messaging.kafka import KafkaEventPublisher, ShipmentConsumer
from src.infrastructure.messaging.outbox_worker import OutboxWorker
from src.infrastructure.persistence.database import get_session_factory
from src.infrastructure.persistence.uow import UnitOfWorkImpl
from src.settings import Settings, get_settings

logger = logging.getLogger(__name__)


class WorkerRuntime:
    """Композиция фоновых компонентов для standalone-процесса."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._session_factory = get_session_factory(settings.database_url)
        self._publisher = KafkaEventPublisher(settings)
        self._notifications_client = NotificationsClientImpl(settings)
        self._dispatcher = NotificationDispatcher(self._notifications_client)

        self._outbox_worker = OutboxWorker(
            uow_factory=self._new_uow,
            publisher=self._publisher,
            settings=settings,
        )
        self._shipment_consumer = ShipmentConsumer(
            settings=settings,
            handler=self._handle_shipment_event,
        )

    def _new_uow(self) -> UnitOfWorkImpl:
        return UnitOfWorkImpl(self._session_factory)

    async def _handle_shipment_event(self, event: ShipmentReceivedEvent) -> None:
        """Обработчик событий доставки: новый UoW на каждое сообщение."""
        use_case = HandleShipmentEventUseCase(
            self._new_uow(),
            notifier=self._dispatcher.notifier,
        )
        await use_case(event)

    async def start(self) -> None:
        """Подключиться к Kafka и запустить relay + consumer."""
        try:
            await self._publisher.start()
        except Exception:
            # Сбой брокера не должен убивать процесс: outbox накапливает
            # события, а консьюмер сам переподключается (см. ShipmentConsumer).
            # aiokafka при неудачном bootstrap оставляет созданный продюсер
            # незакрытым — закрываем явно, иначе ресурс утекает на каждом
            # повторе цикла публикации.
            with contextlib.suppress(Exception):
                await self._publisher.stop()
            logger.exception("Failed to start Kafka publisher; retrying via workers")
        await self._outbox_worker.start()
        await self._shipment_consumer.start()
        logger.info("Standalone worker started (outbox relay + shipment consumer)")

    async def stop(self) -> None:
        """Остановить компоненты и освободить ресурсы."""
        await self._shipment_consumer.stop()
        await self._outbox_worker.stop()
        # Сначала дожидаемся фоновых отправок уведомлений — они используют
        # HTTP-клиент Notifications Service, который закрывается ниже.
        await self._dispatcher.wait_pending()
        await self._publisher.stop()
        await self._notifications_client.close()
        engine = self._session_factory.kw["bind"]
        await engine.dispose()
        logger.info("Standalone worker stopped cleanly")


async def run_forever() -> None:
    """Собрать зависимости и крутить фоновые компоненты до сигнала остановки."""
    settings = get_settings()
    runtime = WorkerRuntime(settings)

    loop = asyncio.get_running_loop()
    stop_requested = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):  # Windows
            loop.add_signal_handler(sig, stop_requested.set)

    await runtime.start()
    logger.info(
        "Worker running: outbox relay -> %s, consumer <- %s",
        settings.order_events_topic,
        settings.shipment_events_topic,
    )
    try:
        await stop_requested.wait()
    finally:
        await runtime.stop()


def main() -> None:
    """CLI-входная точка (`python -m src.bin.worker`)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [worker] %(name)s: %(message)s",
    )
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
