import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

from src.application.ports.catalog import CatalogClient
from src.application.ports.uow import BaseUnitOfWork
from src.domain.entities import Order, OrderStatus
from src.domain.exceptions import (
    CatalogUnavailableError,
    InsufficientStockError,
)

logger = logging.getLogger(__name__)


@dataclass
class CreateOrderCommand:
    """Команда на создание заказа (вход Use Case)."""

    user_id: str
    quantity: int
    item_id: str
    idempotency_key: str


class CreateOrderUseCase:
    """Use Case: создание заказа с гарантией идемпотентности."""

    def __init__(self, uow: BaseUnitOfWork, catalog_client: CatalogClient):
        self._uow = uow
        self._catalog_client = catalog_client

    async def __call__(self, command: CreateOrderCommand) -> Order:
        """Выполнить сценарий создания заказа."""
        async with self._uow() as uow:
            # 1. Проверка идемпотентности
            existing_order = await uow.orders.get_by_idempotency_key(
                command.idempotency_key
            )
            if existing_order:
                return existing_order

            # 2. Проверка наличия товара (ошибки Catalog Service — доменные)
            try:
                item = await self._catalog_client.get_item(command.item_id)
            except httpx.HTTPStatusError as e:
                logger.warning(
                    "Catalog error for item %s: %s",
                    command.item_id,
                    e.response.status_code,
                )
                raise CatalogUnavailableError(
                    f"Товар {command.item_id} не найден в каталоге "
                    f"(HTTP {e.response.status_code})."
                ) from e
            except httpx.HTTPError as e:
                logger.warning("Catalog service unavailable: %s", e)
                raise CatalogUnavailableError(f"Catalog Service недоступен: {e}") from e

            if item.available_qty < command.quantity:
                raise InsufficientStockError(
                    f"Недостаточно товара {command.item_id}. "
                    f"Доступно: {item.available_qty}."
                )

            # 3. Создание сущности
            now = datetime.now(UTC)
            order = Order(
                id=str(uuid.uuid4()),
                user_id=command.user_id,
                item_id=command.item_id,
                quantity=command.quantity,
                status=OrderStatus.NEW,
                idempotency_key=command.idempotency_key,
                created_at=now,
                updated_at=now,
            )

            # 4. Сохранение и коммит
            return await uow.orders.create(order)
