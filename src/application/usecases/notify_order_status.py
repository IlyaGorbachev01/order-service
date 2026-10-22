import logging

from src.application.ports.notifications import (
    NotificationClient,
    build_status_notification,
)
from src.domain.entities import Order
from src.domain.exceptions import NotificationServiceError

logger = logging.getLogger(__name__)


class OrderNotifier:
    """Best-effort отправка уведомления о текущем статусе заказа.

    - уведомление отправляется при каждом изменении статуса заказа
      (NEW / PAID / SHIPPED / CANCELLED);
    - ошибки Notifications Service лишь логируются и НЕ блокируют
      основной процесс обработки заказа;
    - идемпотентность обеспечивается детерминированным ключом
      `{order_id}:{status}` — повторная отправка того же перехода не
      создаёт дубликат на стороне сервиса.

    Клиент необязателен (может быть None в тестах/при отключённой
    интеграции) — тогда нотификация молча пропускается.
    """

    def __init__(self, client: NotificationClient | None = None):
        self._client = client

    async def notify(self, order: Order) -> None:
        """Отправить уведомление о текущем статусе заказа (не бросает исключений)."""
        if self._client is None:
            return
        request = build_status_notification(order)
        try:
            await self._client.send_notification(request)
        except NotificationServiceError as e:
            # Ожидаемый сбой сервиса: логируем, но не срываем бизнес-процесс.
            logger.warning(
                "Failed to send %s notification for order %s: %s",
                order.status.value,
                order.id,
                e,
            )
        except Exception:
            # Любая неожиданная ошибка клиента также не должна влиять
            # на основной сценарий работы с заказом.
            logger.exception(
                "Unexpected error while sending notification for order %s",
                order.id,
            )
