from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal

from src.domain.entities import PaymentStatus


@dataclass(frozen=True)
class PaymentRequest:
    """Параметры создания платежа во внешнем Payments Service."""

    order_id: str
    amount: Decimal
    callback_url: str
    idempotency_key: str


@dataclass(frozen=True)
class PaymentResult:
    """Результат создания платежа, возвращённый Payments Service."""

    id: str
    order_id: str
    amount: Decimal
    status: PaymentStatus


class PaymentsClient(ABC):
    """Абстрактный клиент Payments Service.

    Реализация — в `infrastructure/http/payments.py`. Бросает
    `PaymentServiceError` при недоступности сервиса или отклонении платежа;
    use case не должен знать об HTTP-деталях.
    """

    @abstractmethod
    async def create_payment(self, request: PaymentRequest) -> PaymentResult:
        """Создать платёж для заказа.

        Платёж обрабатывается асинхронно: итоговый статус приходит на
        `callback_url`, указанный в запросе.
        """
