"""Точка запуска outbox-воркера отдельным процессом.

Назначение: вынести публикацию событий Outbox → Kafka из процесса API,
чтобы фоновая работа не конкурировала с обработкой HTTP-запросов за CPU
event loop'а и ресурсы соединения с брокером.

Запуск (тот же образ, что и у API; переменные окружения — те же):

    python -m src.bin.outbox_worker        # или uv run ... в кластере

В текущем деплое LMS (один контейнер `run.sh` = alembic + uvicorn) воркер
продолжает работать фоновой задачей внутри процесса API — этот модуль
предназначен для опционального отдельного Deployment'а. Несколько копий
воркера (и копия в API-процессе одновременно с внешними) безопасны:
партии захватываются транзакцией `FOR UPDATE SKIP LOCKED`, поэтому одно
событие не публикуется двумя экземплярами параллельно.

Graceful shutdown: SIGTERM/SIGINT останавливает цикл после текущей
итерации (не более `outbox_poll_interval` ожидания).
"""

import asyncio
import contextlib
import logging
import signal

from src.infrastructure.messaging.kafka import KafkaEventPublisher
from src.infrastructure.messaging.outbox_worker import OutboxWorker
from src.infrastructure.persistence.database import get_session_factory
from src.infrastructure.persistence.uow import UnitOfWorkImpl
from src.settings import get_settings

logger = logging.getLogger(__name__)


async def run_forever() -> None:
    """Собрать зависимости и крутить outbox-цикл до сигнала остановки."""
    settings = get_settings()
    session_factory = get_session_factory(settings.database_url)
    publisher = KafkaEventPublisher(settings)
    await publisher.start()

    worker = OutboxWorker(
        uow_factory=lambda: UnitOfWorkImpl(session_factory),
        publisher=publisher,
        settings=settings,
    )

    loop = asyncio.get_running_loop()
    stop_requested = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):  # Windows
            loop.add_signal_handler(sig, stop_requested.set)

    await worker.start()
    logger.info("Standalone outbox worker running (pid entrypoint)")
    try:
        await stop_requested.wait()
    finally:
        await worker.stop()
        await publisher.stop()
        engine = session_factory.kw["bind"]
        await engine.dispose()
        logger.info("Standalone outbox worker stopped cleanly")


def main() -> None:
    """CLI-входная точка (`python -m src.bin.outbox_worker`)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [outbox-worker] %(name)s: %(message)s",
    )
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
