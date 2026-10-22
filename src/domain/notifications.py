"""Доменная логика текстов уведомлений и ключей идемпотентности.

Чистый domain-слой: не знает ни про HTTP, ни про порты application слоя.
Тексты соответствуют примерам из Шага 4 задания:

- NEW:       "Ваш заказ создан и ожидает оплаты"
- PAID:      "Ваш заказ успешно оплачен и готов к отправке"
- SHIPPED:   "Ваш заказ отправлен в доставку"
- CANCELLED: "Ваш заказ отменен. Причина: {reason}"
"""

from src.domain.entities import OrderStatus

_STATUS_MESSAGES: dict[OrderStatus, str] = {
    OrderStatus.NEW: "Ваш заказ создан и ожидает оплаты",
    OrderStatus.PAID: "Ваш заказ успешно оплачен и готов к отправке",
    OrderStatus.SHIPPED: "Ваш заказ отправлен в доставку",
    OrderStatus.CANCELLED: "Ваш заказ отменен",
}

DEFAULT_CANCEL_REASON = "не указана"


def notification_message(status: OrderStatus, reason: str | None = None) -> str:
    """Текст уведомления для перехода заказа в статус `status`.

    Для CANCELLED причина добавляется в формате задания:
    "Ваш заказ отменен. Причина: {reason}". Если причина не передана —
    подставляется заглушка, чтобы текст всегда оставался осмысленным.
    """
    base = _STATUS_MESSAGES[status]
    if status is OrderStatus.CANCELLED:
        return f"{base}. Причина: {reason or DEFAULT_CANCEL_REASON}"
    return base


def notification_idempotency_key(order_id: str, status: OrderStatus) -> str:
    """Детерминированный ключ идемпотентности: один заказ + один статус.

    Даже при повторной доставке callback'а или Kafka-события уведомление
    с тем же ключом не создаёт дубликат на стороне Notifications Service.
    """
    return f"{order_id}:{status.value}"
