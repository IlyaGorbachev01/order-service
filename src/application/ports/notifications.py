from abc import ABC, abstractmethod
from dataclasses import dataclass

from src.domain.entities import Order
from src.domain.notifications import notification_idempotency_key, notification_message  # type: ignore


@dataclass(frozen=True)
class NotificationRequest:
    """Параметры отправки уведомления во внешний Notifications Service."""

    message: str
    user_id: str  # получатель уведомления (Notifications Service требует его)
    reference_id: str  # order_id, к которому относится уведомление
    idempotency_key: str  # предотвращает дублирование уведомлений


def build_status_notification(order: Order) -> NotificationRequest:
    """Собрать уведомление о текущем статусе заказа (домен + контракт API).

    Текст берётся из domain-маппинга (`src/domain/notifications.py`), для
    CANCELLED используется причина отмены заказа. Ключ идемпотентности
    детерминирован (`{order_id}:{status}`), поэтому повторная обработка
    того же перехода не создаёт дубликат уведомления. `user_id` — заказ
    сервиса привязывает уведомление к пользователю.
    """
    return NotificationRequest(
        message=notification_message(order.status, reason=order.cancel_reason),
        user_id=order.user_id,
        reference_id=order.id,
        idempotency_key=notification_idempotency_key(order.id, order.status),
    )


class NotificationClient(ABC):
    """Абстрактный клиент Notifications Service.

    Реализация — в `infrastructure/http/notifications.py`.

    Отправка уведомлений не должна блокировать основной процесс обработки
    заказа: use cases вызывают клиента «best effort» и лишь логируют
    ошибки (см. Шаг 4 задания). Исключение `NotificationServiceError`
    бросается реализацией при недоступности сервиса или ошибке HTTP,
    но обязано перехватываться вызывающей стороной.
    """

    @abstractmethod
    async def send_notification(self, request: NotificationRequest) -> None:
        """Отправить уведомление пользователю.

        Идемпотентность обеспечивается полем `idempotency_key`: повторная
        отправка с тем же ключом не создаёт дубликат уведомления.
        """
