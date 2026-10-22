import asyncio
import atexit
import logging
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING

from src.application.ports.notifications import (
    NotificationClient,
    NotificationRequest,
    build_status_notification,
)
from src.domain.entities import Order
from src.domain.exceptions import NotificationServiceError
from src.infrastructure.http.notifications import NotificationsClientImpl
from src.settings import get_settings


def _default_client_factory() -> NotificationClient:
    """Создать стандартный клиент Notifications Service из настроек."""
    return NotificationsClientImpl(get_settings())


if TYPE_CHECKING:
    from concurrent.futures import Future

logger = logging.getLogger(__name__)

# Повторы при сбоях Notifications Service: сервис может быть недоступен
# или временно возвращать ошибку; best-effort-доставка с ретраями и
# линейной задержкой повышает надёжность, не блокируя основной процесс.
_MAX_SEND_ATTEMPTS = 3
_RETRY_BASE_DELAY_SECONDS = 0.5

# Сколько ждать завершения фоновых отправок при остановке процесса.
_SHUTDOWN_TIMEOUT_SECONDS = 25.0


class OrderNotifier:
    """Best-effort отправка уведомления о текущем статусе заказа.

    - уведомление отправляется при каждом изменении статуса заказа
      (NEW / PAID / SHIPPED / CANCELLED);
    - ошибки Notifications Service лишь логируются и НЕ блокируют
      основной процесс обработки заказа;
    - идемпотентность обеспечивается детерминированным ключом
      `{order_id}:{status}` — повторная отправка того же перехода не
      создаёт дубликат на стороне сервиса, поэтому безопасны и повторы
      после сетевых сбоев (at-least-once + дедупликация по ключу).

    Реализация — отдельный потоковый event loop с собственным экземпляром
    HTTP-клиента: очередь задач переживает завершение любого HTTP-запроса
    и работает независимо от цикла lifespan-задач (consumer/outbox).
    Это устраняет классическую потерю «фоновых» уведомлений: задача,
    запланированная через `asyncio.create_task`, могла быть сборана GC
    или никогда не получить планировщик, а вызов из потока консьюмера
    вообще падал с RuntimeError («no running event loop») — такие
    переходы (например, SHIPPED) терялись без какого-либо лога об ошибке
    отправки.

    Клиент необязателен (может быть None в тестах/при отключённой
    интеграции) — тогда нотификация молча пропускается.
    """

    def __init__(self, client_factory: Callable[[], NotificationClient] | None = None):
        """Создать нотификатор.

        `client_factory` вызывается лениво в фоновом потоке — конструктор
        остаётся синхронным и не требует запущенного event loop. Если
        фабрика не передана явно, используется стандартная реализация
        (NotificationsClientImpl из настроек приложения); это позволяет
        создавать нотификатор «по умолчанию» в use case'ах без явной
        сборки зависимостей.
        """
        if client_factory is None:
            client_factory = _default_client_factory
        self._client_factory = client_factory
        self._client: NotificationClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = False
        self._lock = threading.Lock()
        self._futures: set[Future] = set()

    # --- инфраструктура фона ------------------------------------------------

    def _ensure_started(self) -> bool:
        """Поднять фоновый поток с event loop (лениво, один раз).

        Возвращает False, если клиент не настроен — тогда нотификация
        пропускается (интеграция отключена).

        Если поток или loop погибли (например, graceful shutdown убил
        loop, а процесс продолжает работать), состояние сбрасывается и
        фон поднимается заново — уведомление не теряется молча.
        """
        if self._started and self._is_alive():
            return True
        with self._lock:
            if self._started and self._is_alive():
                return True
            # Сброс «мёртвого» состояния перед новым стартом.
            self._started = False
            self._loop = None
            self._thread = None
            self._client = None
            if self._client_factory is None:
                return False
            loop = asyncio.new_event_loop()
            ready = threading.Event()

            def _run_loop() -> None:
                asyncio.set_event_loop(loop)
                try:
                    self._client = self._client_factory()
                except Exception:
                    logger.exception("Failed to create notifications client")
                finally:
                    ready.set()
                loop.run_forever()

            thread = threading.Thread(
                target=_run_loop, name="order-notifier", daemon=True
            )
            thread.start()
            # Ждём создания клиента: submit возможен только когда loop
            # крутится, а клиент создан до первого send.
            ready.wait(timeout=10.0)
            self._loop = loop
            self._thread = thread
            self._started = True
            atexit.register(self.shutdown)
            logger.info("Order notifier background loop started")
            return True

    def _is_alive(self) -> bool:
        """Фоновый поток жив и его event loop ещё крутится."""
        thread, loop = self._thread, self._loop
        return (
            thread is not None
            and loop is not None
            and thread.is_alive()
            and loop.is_running()
        )

    # --- публичный API --------------------------------------------------------

    def notify(self, order: Order) -> None:
        """Поставить уведомление о статусе заказа в фоновую очередь.

        Не бросает исключений, не блокирует вызывающий код и безопасен
        из любого контекста (HTTP-обработчик, поток Kafka-консьюмера,
        тест без event loop).
        """
        if not self._ensure_started():
            return
        assert self._loop is not None
        request = build_status_notification(order)
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._deliver(request, order.id), self._loop
            )
        except RuntimeError:
            # Loop мог остановиться между проверкой и submit —
            # переподнимаем фон и пробуем ещё раз (не молча теряем).
            logger.warning(
                "Notifier loop stopped mid-submit for order %s; restarting",
                order.id,
            )
            self._started = False
            if not self._ensure_started():
                return
            assert self._loop is not None
            future = asyncio.run_coroutine_threadsafe(
                self._deliver(request, order.id), self._loop
            )
        self._futures.add(future)
        future.add_done_callback(self._futures.discard)

    def shutdown(self, timeout: float = _SHUTDOWN_TIMEOUT_SECONDS) -> None:
        """Дождаться недоставанных уведомлений и остановить фоновый поток.

        Вызывается автоматически при остановке процесса (atexit), а также
        из lifespan-хука приложения, чтобы ни одно уведомление не
        потерялось при graceful shutdown.
        """
        if not self._started:
            return
        loop, thread = self._loop, self._thread
        if loop is None or thread is None:
            return
        pending = [f for f in list(self._futures) if not f.done()]
        for future in pending:
            try:
                future.result(timeout=timeout)
            except Exception:
                logger.exception("Notification delivery failed during shutdown")
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5.0)
        self._started = False

    async def wait_pending(self) -> None:
        """Дождаться всех фоновых отправок (совместимость со старым API).

        Синхронизируется через потоковые futures, поэтому может вызываться
        из любого event loop (в т.ч. тестового).
        """
        for future in list(self._futures):
            try:
                await asyncio.wrap_future(future)
            except Exception:
                logger.exception("Notification delivery error")

    # --- доставка -------------------------------------------------------------

    async def _deliver(
        self, request: NotificationRequest, order_id: str
    ) -> None:
        """Отправить уведомление с ограниченными повторами (best effort)."""
        if self._client is None:
            logger.debug(
                "Notifications client unavailable; skipping notification "
                "for order %s",
                order_id,
            )
            return
        last_error: Exception | None = None
        for attempt in range(1, _MAX_SEND_ATTEMPTS + 1):
            try:
                await self._client.send_notification(request)
                # У уходящего уведомления всегда есть idempotency_key —
                # логируем его явно, чтобы по логам пода можно было
                # подтвердить отправку каждого статуса (NEW/PAID/SHIPPED).
                logger.info(
                    "Notification delivered for order %s "
                    "(status message %r, key=%s)",
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
                delay = _RETRY_BASE_DELAY_SECONDS * attempt
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
