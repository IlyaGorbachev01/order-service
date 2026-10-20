import logging
from decimal import Decimal

import httpx

from src.application.ports.payments import (
    PaymentRequest,
    PaymentResult,
    PaymentsClient,
)
from src.domain.entities import PaymentStatus
from src.domain.exceptions import PaymentServiceError
from src.settings import Settings

logger = logging.getLogger(__name__)


class PaymentsClientImpl(PaymentsClient):
    """HTTP-клиент для создания платежей во внешнем Payments Service."""

    def __init__(self, settings: Settings):
        self._base_url = settings.capashino_internal_url
        self._api_key = settings.capashino_api_token
        self._client = httpx.AsyncClient(timeout=10.0)

    async def create_payment(self, request: PaymentRequest) -> PaymentResult:
        """Создать платёж через POST /api/payments.

        Бросает PaymentServiceError при любой ошибке сервиса (HTTP-статус,
        недоступность, неожиданный формат ответа), чтобы use case мог
        отменить заказ и не знал об HTTP-деталях.
        """
        url = f"{self._base_url}/api/payments"
        headers = {"X-API-Key": self._api_key}
        payload = {
            "order_id": request.order_id,
            "amount": str(request.amount),
            "callback_url": request.callback_url,
            "idempotency_key": request.idempotency_key,
        }

        try:
            response = await self._client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Payments error for order %s: HTTP %s",
                request.order_id,
                e.response.status_code,
            )
            raise PaymentServiceError(
                f"Payments Service отклонил создание платежа "
                f"(HTTP {e.response.status_code})."
            ) from e
        except httpx.HTTPError as e:
            logger.warning("Payments service unavailable: %s", e)
            raise PaymentServiceError(f"Payments Service недоступен: {e}") from e

        try:
            return PaymentResult(
                id=str(data["id"]),
                order_id=str(data["order_id"]),
                amount=Decimal(str(data["amount"])),
                status=PaymentStatus(str(data["status"])),
            )
        except (KeyError, ValueError) as e:
            logger.error("Unexpected Payments Service response: %r", data)
            raise PaymentServiceError(
                f"Некорректный ответ Payments Service: {e}"
            ) from e

    async def close(self) -> None:
        """Закрывает HTTP-клиент и освобождает ресурсы."""
        await self._client.aclose()
