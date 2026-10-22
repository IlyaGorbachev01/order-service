import logging

import httpx

from src.application.ports.notifications import (
    NotificationClient,
    NotificationRequest,
)
from src.domain.exceptions import NotificationServiceError
from src.settings import Settings

logger = logging.getLogger(__name__)


class NotificationsClientImpl(NotificationClient):
    """HTTP-клиент для отправки уведомлений во внешний Notifications Service.

    Бросает NotificationServiceError при любой ошибке сервиса (HTTP-статус,
    недоступность, неожиданный формат ответа), чтобы use case мог перехватить
    её и лишь залогировать — уведомления не блокируют основной процесс.
    """

    def __init__(self, settings: Settings):
        self._base_url = settings.notifications_base_url
        self._api_key = settings.notifications_api_key
        self._client = httpx.AsyncClient(timeout=settings.http_timeout)

    async def send_notification(self, request: NotificationRequest) -> None:
        """Отправить уведомление через POST /api/notifications."""
        url = f"{self._base_url}/api/notifications"
        headers = {"X-API-Key": self._api_key}
        payload = {
            "message": request.message,
            "reference_id": request.reference_id,
            "idempotency_key": request.idempotency_key,
        }

        try:
            response = await self._client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Notifications error for order %s: HTTP %s",
                request.reference_id,
                e.response.status_code,
            )
            raise NotificationServiceError(
                f"Notifications Service отклонил отправку уведомления "
                f"(HTTP {e.response.status_code})."
            ) from e
        except httpx.HTTPError as e:
            logger.warning("Notifications service unavailable: %s", e)
            raise NotificationServiceError(
                f"Notifications Service недоступен: {e}"
            ) from e

        notification_id = data.get("id") if isinstance(data, dict) else None
        logger.info(
            "Notification sent for order %s (id=%s, key=%s)",
            request.reference_id,
            notification_id,
            request.idempotency_key,
        )

    async def close(self) -> None:
        """Закрывает HTTP-клиент и освобождает ресурсы."""
        await self._client.aclose()
