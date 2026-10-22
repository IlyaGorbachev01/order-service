from abc import ABC, abstractmethod
from dataclasses import dataclass

from src.domain.entities import OrderStatus


@dataclass(frozen=True)
class NotificationRequest:
    """Параметры отправки уведомления во внешний Notifications Service."""

    message: str
    reference_id: str  # order_id, к которому относится уведомление
    idempotency_key: str  # предотвращает дублирование уведомлений


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


def notification_idempotency_key(order_id: str, status: OrderStatus) -> str:
    """Детерминированный ключ идемпотентности для уведомления о статусе.

    Один заказ + один статус = ровно одно уведомление, даже если
    обработчик (callback / Kafka-событие) выполнится несколько раз.
    """
    return f"{order_id}:{status.value}"
