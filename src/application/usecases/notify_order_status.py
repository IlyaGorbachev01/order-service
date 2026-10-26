import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

from src.application.ports.notifications import (
    NotificationClient,
    NotificationRequest,
    build_status_notification,
)
from src.application.usecases.create_order import NotificationPort
from src.domain.entities import Order
from src.domain.exceptions import NotificationServiceError
from src.domain.notifications import normalize_user_id

logger = logging.getLogger(__name__)

# Повторы при сбоях Notifications Service: сервис может быть недоступен
# или временно возвращать ошибку; best-effort-доставка с ретраями и
# квадратичным backoff (0.5с, 2.0с) повышает надёжность, не блокируя
# основной процесс.
_MAX_SEND_ATTEMPTS = 3
_RETRY_BASE_DELAY_SECONDS = 0.5


class OrderNotifier(NotificationPort):
    """Best-effort отправка уведомления о текущем статусе заказа.

    - уведомление отправляется при каждом изменении статуса заказа
      (NEW / PAID / SHIPPED / CANCELLED);
    - ошибки Notifications Service лишь логируются и НЕ блокируют
      основной процесс обработки заказа;
    - идемпотентность обеспечивается детерминированным ключом
      `{order_id}:{status}` — повторная отправка того же перехода не
      создаёт дубликат на стороне сервиса, поэтому безопасны и повторы
      после сетевых сбоев (at-least-once + дедупликация по ключу).

    Клиент (`NotificationClient`) передаётся СТРОГО через конструктор из
    композиционного корня (`presentation/api/dependencies.py`,
    `src/bin/worker.py`): use case и этот нотификатор ничего не знают об
    инфраструктурной реализации клиента (правило зависимостей чистой
    архитектуры; HTTP-детали транслирует `infrastructure/http/`).

    Планирование фоновой доставки делегировано инъектируемому
    `scheduler`: в HTTP-процессе это `BackgroundTasks.add_task` (задача
    гарантированно выполняется после ответа клиенту), в worker-процессе —
    `asyncio.create_task` в основном event loop. Это заменяет прежнюю
    избыточную инфраструктуру (отдельный поток + свой event loop + atexit
    + самоперезапуск): обе точки запуска уже крутят свои asyncio циклы,
    поэтому фоновый поток не нужен. Если планировщик не передан, задача
    заводится в текущем event loop; вне loop уведомление пропускается с
    логом — ни один переход статуса не должен ронять основной сценарий.
    """

    def __init__(
        self,
        client: NotificationClient | None,
        scheduler: Callable[[Awaitable[None]], None] | None = None,
    ):
        """Создать нотификатор.

        `client` — порт Notifications Service (может быть None, если
        интеграция отключена — тогда нотификация пропускается).
        `scheduler` — функция постановки корутины доставки в фон
        (например, `BackgroundTasks.add_task` или `asyncio.create_task`).
        """
        self._client = client
        self._scheduler = scheduler

    # --- публичный API --------------------------------------------------------

    async def notify(self, order: Order) -> None:
        """Поставить уведомление о статусе заказа в фон.

        Не бросает исключений и безопасна из любого контекста
        (HTTP-обработчик, консьюмер Kafka, тест).

        Планирование зависит от типа планировщика (определяется по
        сигнатуре `add_task`, без импортов presentation/infrastructure):

        - ``BackgroundTasks.add_task`` (HTTP-процесс): принимает СИНХРОННЫЙ
          коллбэк, который starlette выполняет через run_in_threadpool
          ПОСЛЕ отправки ответа; передача корутины привела бы к
          TypeError ("the first argument must be callable") уже после
          ответа клиенту — так терялись уведомления NEW/PAID (провалившийся
          шаг автотеста: 3 ожидаемых уведомления, получено 2). Поэтому в
          background кладётся sync-обёртка `_spawn_or_run`, которая заводит
          доставку task'ом обратно в основной event loop процесса.
        - иной планировщик (worker-процесс: InflightTracker.schedule,
          ожидающий корутину): доставляем корутиной напрямую — тот же
          event loop, без лишних потоков и перепланирования.
        """
        if self._client is None:
            logger.debug(
                "Notifications client not configured; skipping notification "
                "for order %s",
                order.id,
            )
            return
        request = self._build_request(order)
        if self._scheduler is not None:
            if self._is_starlette_background(self._scheduler):
                # Фоновый режим HTTP-запроса: sync-коллбэк в threadpool,
                # доставка — task'ом в основном loop (см. docstring).
                def _background_deliver(
                    req: NotificationRequest = request, oid: str = order.id
                ) -> None:
                    self._spawn_or_run(req, oid)

                planned: Any = _background_deliver
            else:
                # Worker-процесс: трекер ждёт именно корутину доставки.
                planned = self._deliver(request, order.id)
            try:
                self._scheduler(planned)
                return
            except Exception:  # pragma: no cover - отказ планировщика
                logger.exception(
                    "Notification scheduler failed for order %s; delivering inline",
                    order.id,
                )
                # Корутина могла быть «съедена» планировщиком — пересобираем.
                request = self._build_request(order)
        # Синхронный фолбэк: планировщика нет, но event loop запущен —
        # доставляем прямо здесь (best-effort, ошибки только логируются).
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            logger.warning(
                "No running event loop and no scheduler: dropping notification "
                "for order %s (status %s)",
                order.id,
                order.status.value,
            )
            return
        await self._deliver(request, order.id)

    @staticmethod
    def _is_starlette_background(add_task: Callable[..., Any]) -> bool:
        """Отличить `BackgroundTasks.add_task` от обычного планировщика.

        У starlette/FastAPI `add_task` принимает первым именованным
        параметром `func` плюс `*args/**kwargs`; у наших планировщиков
        (InflightTracker.schedule) — единственный позиционный параметр
        корутины. Проверка по сигнатуре не создаёт импорт presentation
        или starlette в application-слое.
        """
        try:
            params = inspect.signature(add_task).parameters
        except (TypeError, ValueError):  # pragma: no cover - builtins/C-func
            return False
        values = list(params.values())
        return (
            bool(values)
            and values[0].name == "func"
            and any(p.kind is p.VAR_POSITIONAL for p in values)
        )

    # --- доставка -------------------------------------------------------------

    @staticmethod
    def _build_request(order: Order) -> NotificationRequest:
        """Собрать запрос уведомления (текст — домен, user_id — UUID).

        user_id нормализуется в UUID здесь (на границе отправки), а не при
        записи заказа: иначе Payments Service получал бы чужой uuid5 вместо
        исходного "user_1" и тест сверял бы платёж/доставку по другому id.
        """
        request = build_status_notification(order)
        return replace(request, user_id=normalize_user_id(request.user_id))

    async def deliver(self, order: Order) -> None:
        """Синхронно (в текущем loop) доставить уведомление о статусе заказа.

        Идемпотентность повторной доставки обеспечивает ключ
        `{order_id}:{status}` на стороне Notifications Service.
        """
        await self._deliver(self._build_request(order), order.id)

    def _spawn_or_run(self, request: NotificationRequest, order_id: str) -> None:
        """Вызывается СИНХРОННО из фона (threadpool у BackgroundTasks).

        Пытается завести доставку task'ом в основном event loop процесса
        (fire-and-forget; ошибки гасятся внутри `_deliver`). Если loop уже
        недоступен (остановка приложения) — выполняем доставку синхронно в
        этом же потоке через временный event loop: best-effort гарантия,
        что уведомление не потеряется молча.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None and not loop.is_closed():
            loop.create_task(self._deliver(request, order_id))
            return
        # Loop нет (task запущен starlette в threadpool после завершения
        # ответа) — крутим короткий локальный loop только под эту доставку.
        try:
            asyncio.run(self._deliver(request, order_id))
        except Exception:  # pragma: no cover - отказ страховочного прогона
            logger.exception(
                "Inline fallback notification delivery failed for order %s",
                order_id,
            )

    async def _deliver(self, request: NotificationRequest, order_id: str) -> None:
        """Отправить уведомление с ограниченными повторами (best effort)."""
        assert self._client is not None
        last_error: Exception | None = None
        for attempt in range(1, _MAX_SEND_ATTEMPTS + 1):
            try:
                await self._client.send_notification(request)
                # У уходящего уведомления всегда есть idempotency_key —
                # логируем его явно, чтобы по логам пода можно было
                # подтвердить отправку каждого статуса (NEW/PAID/SHIPPED).
                logger.info(
                    "Notification delivered for order %s (status message %r, key=%s)",
                    order_id,
                    request.message,
                    request.idempotency_key,
                )
                return
            except NotificationServiceError as e:
                # Ожидаемый сбой сервиса: повторяем, затем логируем.
                last_error = e
            except Exception as e:
                # Любая неожиданная ошибка клиента также не должна влиять
                # на основной сценарий работы с заказом.
                last_error = e
            if attempt < _MAX_SEND_ATTEMPTS:
                delay = _RETRY_BASE_DELAY_SECONDS * (attempt**2)
                logger.info(
                    "Notification for order %s failed (attempt %d/%d), "
                    "retrying in %.1fs: %s",
                    order_id,
                    attempt,
                    _MAX_SEND_ATTEMPTS,
                    delay,
                    last_error,
                )
                await asyncio.sleep(delay)
        logger.warning(
            "Giving up on notification for order %s after %d attempts: %s",
            order_id,
            _MAX_SEND_ATTEMPTS,
            last_error,
        )
