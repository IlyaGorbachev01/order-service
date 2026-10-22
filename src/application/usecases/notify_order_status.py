import asyncio
import logging

from src.application.ports.notifications import (
    NotificationClient,
    NotificationRequest,
    build_status_notification,
)
from src.domain.entities import Order
from src.domain.exceptions import NotificationServiceError

logger = logging.getLogger(__name__)

# Повторы при сбоях Notifications Service
_MAX_SEND_ATTEMPTS = 3
_RETRY_BASE_DELAY_SECONDS = 0.5


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

    Отправка выполняется фоновой задачей (`asyncio.create_task`), чтобы
    медленный/недоступный Notifications Service не задерживал HTTP-ответ
    и обработку Kafka-событий. Задача удерживается сильной ссылкой до
    завершения — иначе event loop мог бы собрать её GC'ем.

    Клиент необязателен (может быть None в тестах/при отключённой
    интеграции) — тогда нотификация молча пропускается.
    """

    def __init__(self, client: NotificationClient | None = None):
        self._client = client
        self._tasks: set[asyncio.Task[None]] = set()

    def notify(self, order: Order) -> None:
        """Запланировать отправку уведомления о статусе заказа.

        Не бросает исключений и не ждёт ответа сервиса — доставка
        выполняется фоново с повторами (см. `_deliver`).
        """
        if self._client is None:
            return
        request = build_status_notification(order)
        task = asyncio.get_running_loop().create_task(self._deliver(request, order.id))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _deliver(self, request: NotificationRequest, order_id: str) -> None:
        """Отправить уведомление с ограниченными повторами (best effort)."""
        assert self._client is not None
        last_error: Exception | None = None
        for attempt in range(1, _MAX_SEND_ATTEMPTS + 1):
            try:
                await self._client.send_notification(request)
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

    async def wait_pending(self) -> None:
        """Дождаться всех фоновых отправок (используется в тестах/shutdown)."""
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
