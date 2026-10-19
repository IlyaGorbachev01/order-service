from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class CatalogItem:
    """Позиция каталога."""
    id: str
    name: str
    price: Decimal
    available_qty: int


class CatalogClient(ABC):
    """Абстрактный клиент каталога товаров."""
    @abstractmethod
    async def get_item(self, item_id: str) -> CatalogItem:
        """Получить информацию о товаре из каталога."""
