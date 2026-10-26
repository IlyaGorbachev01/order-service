import logging
from decimal import Decimal, InvalidOperation
from http import HTTPStatus

import httpx

from src.application.ports.catalog import CatalogClient, CatalogItem
from src.domain.exceptions import (
    CatalogUnavailableError,
    ItemNotFoundError,
)
from src.settings import Settings

logger = logging.getLogger(__name__)


class CatalogClientImpl(CatalogClient):
    """HTTP-клиент для получения данных из каталога.

    Все HTTP-детали (`httpx.HTTPStatusError`, сетевые сбои, некорректный
    payload) транслируются здесь в доменные исключения — `ItemNotFoundError`
    (404: «товара нет», штатная бизнес-ситуация) и `CatalogUnavailableError`
    (прочие ошибки). Слой application работает только с доменными ошибками и
    не импортирует httpx (правило зависимостей чистой архитектуры).
    """

    def __init__(self, settings: Settings):
        self._base_url = settings.capashino_internal_url
        self._api_key = settings.capashino_api_token
        self._client = httpx.AsyncClient(timeout=settings.http_timeout)

    async def get_item(self, item_id: str) -> CatalogItem:
        """Получает товар из каталога по идентификатору."""
        url = f"{self._base_url}/api/catalog/items/{item_id}"
        headers = {"X-API-Key": self._api_key}

        try:
            response = await self._client.get(url, headers=headers)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            logger.warning("Catalog error for item %s: HTTP %s", item_id, status_code)
            if status_code == HTTPStatus.NOT_FOUND:
                raise ItemNotFoundError(f"Товар {item_id} не найден в каталоге.") from e
            raise CatalogUnavailableError(
                f"Catalog Service вернул ошибку HTTP {status_code} "
                f"для товара {item_id}."
            ) from e
        except httpx.HTTPError as e:
            logger.warning("Catalog service unavailable: %s", e)
            raise CatalogUnavailableError(f"Catalog Service недоступен: {e}") from e

        try:
            data = response.json()
        except ValueError as e:
            # Нестандартный ответ (невалидный JSON — например HTML-страница
            # ошибки от ingress/прокси при 200). Без перевода в доменное
            # исключение это дало бы 500 Internal Server Error.
            logger.warning(
                "Catalog returned non-JSON body for item %s: %.200r",
                item_id,
                response.text,
            )
            raise CatalogUnavailableError(
                f"Catalog Service вернул некорректный ответ для товара {item_id}."
            ) from e
        if not isinstance(data, dict):
            raise CatalogUnavailableError(
                f"Catalog Service вернул неожиданный формат ответа "
                f"для товара {item_id}."
            )

        try:
            return CatalogItem(
                id=str(data["id"]),
                name=str(data["name"]),
                price=Decimal(str(data["price"])),
                available_qty=int(data["available_qty"]),
            )
        except (KeyError, TypeError, ValueError, InvalidOperation) as e:
            # Неполный/неконвертируемый JSON — также не должен превращаться
            # в 500: переводим в доменную ошибку недоступности каталога.
            logger.warning(
                "Catalog returned unexpected payload for item %s: %r", item_id, data
            )
            raise CatalogUnavailableError(
                f"Catalog Service вернул некорректные данные товара {item_id}: {e}"
            ) from e

    async def close(self) -> None:
        """Закрывает HTTP-клиент и освобождает ресурсы."""
        await self._client.aclose()
