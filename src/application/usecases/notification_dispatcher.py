"""Центральный диспетчер уведомлений о статусах заказов.

Все use case'ы (создание заказа, callback платежа, события доставки)
используют ОДИН экземпляр OrderNotifier: фоновые задачи отправки
переживают завершение HTTP-запроса/обработки события и могут быть
дожиданы при остановке приложения (`wait_pending`), чтобы ни одно
уведомление не потерялось.

Композиция корня (`presentation/api/dependencies.py`, `src/bin/worker.py`)
передаёт сюда инфраструктурный клиент `NotificationClient` и планировщик
фоновых задач; сам диспетчер импортов infrastructure не делает — use
case'ы зависят только от абстрактного порта `NotificationPort`.
"""

import asyncio
import logging
from collections.abc import Awaitable

from src.application.ports.notifications import NotificationClient
from src.application.usecases.notify_order_status import OrderNotifier

logger = logging.getLogger(__name__)


class InflightTracker:
    """Учёт задач доставки уведомлений «в полёта».

    Планировщик worker-процесса оборачивает каждую доставку так, что
    asyncio.Task попадает в множество `pending` до начала выполнения и
    снимается по завершении. Это даёт graceful shutdown возможность
    дождаться всех недоставанных уведомлений до закрытия HTTP-клиента
    Notifications Service (иначе последние переходы статусов терялись
    молча).

    Храним именно Task, а не корутину: `asyncio.gather` умеет ждать
    задачи повторно даже после их завершения (завершённый gather не
    создаёт новых задач из уже запущенных Tasks), тогда как повторный
    gather по одной и той же корутины бросал бы
    ``TypeError: object function can't be used in 'await' expression``
    (см. падение консьюмера на доставке SHIPPED-уведомления).
    """

    def __init__(self) -> None:
        self.pending: set[asyncio.Task] = set()

    def schedule(self, coro: Awaitable[None]) -> None:
        """Поставить корутину фоновой задачей в текущем event loop."""
        task = asyncio.ensure_future(coro)
        self.pending.add(task)
        task.add_done_callback(self._done)

    def _done(self, task: asyncio.Task) -> None:
        self.pending.discard(task)
        self._log_error(task)

    @staticmethod
    def _log_error(task: asyncio.Task) -> None:
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.error("Background notification task failed: %s", exc)

    async def wait_pending(self) -> None:
        """Дождаться всех незавершённых доставок."""
        while self.pending:
            await asyncio.gather(*list(self.pending), return_exceptions=True)


class NotificationDispatcher:
    """Синглтон OrderNotifier + учёт фоновых доставок для всего процесса."""

    def __init__(
        self,
        client: NotificationClient | None,
        tracker: InflightTracker | None = None,
    ):
        self._client = client
        # Worker-процесс использует общий трекер «в полёта» для graceful
        # shutdown; в HTTP-процессе трекера нет — там планирование делает
        # BackgroundTasks per-request (см. presentation/api/routes).
        self._tracker = tracker
        self._notifier: OrderNotifier | None = None

    @property
    def notifier(self) -> OrderNotifier:
        """Асинхронная отправка уведомлений об изменении статуса заказа."""
        if self._notifier is None:
            scheduler = self._tracker.schedule if self._tracker is not None else None
            self._notifier = OrderNotifier(self._client, scheduler=scheduler)
        return self._notifier

    def request_notifier(self, background: object) -> OrderNotifier:
        """Собрать нотификатор на текущий HTTP-запрос.

        Планировщиком выступает `BackgroundTasks` (FastAPI/starlette):
        доставка выполняется после отправки ответа клиенту и гарантированно
        не теряется при сборке GC (прежний потоковый event loop для этого
        не нужен). Ошибки доставки гасятся внутри OrderNotifier
        (best-effort).
        """
        add_task = getattr(background, "add_task", None)
        if not callable(add_task):  # pragma: no cover - защита от misuse
            raise TypeError("background must provide add_task()")
        return OrderNotifier(self._client, scheduler=add_task)

    async def wait_pending(self) -> None:
        """Дождаться завершения всех отложенных отправок (graceful shutdown)."""
        if self._tracker is not None:
            await self._tracker.wait_pending()
