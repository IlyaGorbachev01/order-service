from decimal import Decimal

import httpx

from src.application.ports.catalog import CatalogClient, CatalogItem
from src.settings import Settings


class CatalogClientImpl(CatalogClient):
    """HTTP-клиент для получения данных из каталога."""

    def __init__(self, settings: Settings):
        self._base_url = settings.capashino_internal_url
        self._api_key = settings.capashino_api_token
        self._client = httpx.AsyncClient(timeout=10.0)

    async def get_item(self, item_id: str) -> CatalogItem:
        """Получает товар из каталога по идентификатору."""
        url = f"{self._base_url}/api/catalog/items/{item_id}"
        headers = {"X-API-Key": self._api_key}

        response = await self._client.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        return CatalogItem(
            id=str(data["id"]),
            name=str(data["name"]),
            price=Decimal(str(data["price"])),
            available_qty=int(data["available_qty"]),
        )

    async def close(self) -> None:
        """Закрывает HTTP-клиент и освобождает ресурсы."""
        await self._client.aclose()
