import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import httpx

from src.application.ports.catalog import CatalogClient, CatalogItem
from src.application.ports.payments import PaymentRequest, PaymentsClient
from src.application.ports.uow import BaseUnitOfWork
from src.domain.entities import Order, OrderStatus, PaymentStatus
from src.domain.exceptions import (
    CatalogUnavailableError,
    ConcurrentOrderCreationError,
    InsufficientStockError,
    PaymentServiceError,
)

logger = logging.getLogger(__name__)

# Повторы при гонке на уникальном индексе idempotency_key
_MAX_IDEMPOTENCY_RETRIES = 3
_RETRY_BACKOFF_SECONDS = 0.05


@dataclass
class CreateOrderCommand:
    """Команда на создание заказа (вход Use Case)."""

    user_id: str
    quantity: int
    item_id: str
    idempotency_key: str


class CreateOrderUseCase:
    """Use Case: создание заказа с гарантией идемпотентности.

    Шаги 1-2: проверка стока в Catalog Service -> сохранение заказа (NEW)
    -> создание платежа в Payments Service с callback_url этого сервиса.
    При сбое платежа заказ отменяется (CANCELLED).

    Идемпотентность обеспечивается уникальным индексом `idempotency_key`:
    если параллельный запрос (например, на другой реплике пода) уже успел
    создать заказ с тем же ключом, нарушение уникальности переводится в
    повторное чтение и возврат существующего заказа — клиент получает тот
    же order_id вместо 500 Internal Server Error.
    """

    def __init__(
        self,
        uow: BaseUnitOfWork,
        catalog_client: CatalogClient,
        payments_client: PaymentsClient | None = None,
        callback_url: str = "",
    ):
        self._uow = uow
        self._catalog_client = catalog_client
        self._payments_client = payments_client
        self._callback_url = callback_url

    async def __call__(self, command: CreateOrderCommand) -> Order:
        """Выполнить сценарий создания заказа."""
        order, created = await self._create_order(command)
        # Платёж создаём только для нового заказа: повторный запрос с тем же
        # idempotency_key возвращает исходный заказ без побочных эффектов.
        if created and self._payments_client is not None:
            amount = await self._resolve_amount(order)
            order = await self._create_payment(order, amount, self._callback_url)
        return order

    async def _create_order(self, command: CreateOrderCommand) -> tuple[Order, bool]:
        """Сохранить заказ NEW; вернуть (заказ, был_ли_он_создан_этим_запросом).

        Гонка на уникальном индексе `idempotency_key` (параллельные запросы
        или несколько реплик пода) отрабатывается ограниченным числом
        повторов: при конфликте читаем заказ, зафиксированный конкурентом.
        """
        last_error: ConcurrentOrderCreationError | None = None
        for attempt in range(1, _MAX_IDEMPOTENCY_RETRIES + 1):
            try:
                order, created = await self._save_new_order(command)
                return order, created
            except ConcurrentOrderCreationError as e:
                # Уникальный индекс: конкурент уже создал заказ с этим ключом.
                last_error = e
                logger.info(
                    "Idempotency race on key %s (attempt %d/%d): %s",
                    command.idempotency_key,
                    attempt,
                    _MAX_IDEMPOTENCY_RETRIES,
                    e,
                )
                await asyncio.sleep(_RETRY_BACKOFF_SECONDS * attempt)

                existing = await self._find_existing(command.idempotency_key)
                if existing is not None:
                    logger.info(
                        "Returning existing order %s for idempotency_key %s",
                        existing.id,
                        command.idempotency_key,
                    )
                    return existing, False

        # Не удалось дождаться фиксации конкурента — это штатная ситуация
        # конкуренции, а не внутренняя ошибка сервиса (см. обработку в роуте).
        raise last_error  # type: ignore[misc]

    async def _save_new_order(self, command: CreateOrderCommand) -> tuple[Order, bool]:
        """Проверить сток и записать заказ NEW (одна транзакция).

        Возвращает (заказ, создан_сейчас). Для уже существующего заказа
        второй флаг — False, чтобы не дублировать платёж.
        """
        async with self._uow() as uow:
            # 1. Проверка идемпотентности
            existing_order = await uow.orders.get_by_idempotency_key(
                command.idempotency_key
            )
            if existing_order:
                # Заказ уже создан ранее — возвращаем его как есть:
                # платёж для него уже был создан в первом запросе.
                return existing_order, False

            # 2. Проверка наличия товара (ошибки Catalog Service — доменные)
            item = await self._get_catalog_item(command.item_id)

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

            # 4. Сохранение заказа в статусе NEW (коммит транзакции)
            #    Нарушение unique по idempotency_key бросает
            #    ConcurrentOrderCreationError -> повтор в _create_order.
            created = await uow.orders.create(order)
            return created, True

    async def _find_existing(self, idempotency_key: str) -> Order | None:
        """Прочитать заказ, созданный конкурентным запросом."""
        async with self._uow() as uow:
            return await uow.orders.get_by_idempotency_key(idempotency_key)

    async def _get_catalog_item(self, item_id: str) -> CatalogItem:
        """Получить товар из Catalog Service, переводя ошибки в доменные."""
        try:
            return await self._catalog_client.get_item(item_id)
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Catalog error for item %s: %s", item_id, e.response.status_code
            )
            raise CatalogUnavailableError(
                f"Товар {item_id} не найден в каталоге (HTTP {e.response.status_code})."
            ) from e
        except httpx.HTTPError as e:
            logger.warning("Catalog service unavailable: %s", e)
            raise CatalogUnavailableError(f"Catalog Service недоступен: {e}") from e

    async def _resolve_amount(self, order: Order) -> Decimal:
        """Рассчитать сумму платежа по цене товара из каталога."""
        item = await self._get_catalog_item(order.item_id)
        return item.price * order.quantity

    async def _create_payment(
        self, order: Order, amount: Decimal, callback_url: str
    ) -> Order:
        """Создать платёж через Payments Service.

        При недоступности сервиса или отклонении платежа заказ отменяется
        (CANCELLED) и бросается PaymentServiceError. Если сервис вернул
        синхронный успех (succeeded) — заказ сразу переходит в PAID;
        при pending итог придёт на callback_url.
        """
        request = PaymentRequest(
            order_id=order.id,
            amount=amount.quantize(Decimal("0.01")),
            callback_url=callback_url,
            idempotency_key=self._build_payment_idempotency_key(order.id),
        )
        if not callback_url:
            logger.warning(
                "callback_url is empty: set ORDER_SERVICE_INTERNAL_URL env var "
                "(k8s internal host <service>.<namespace>.svc:<port>) so that "
                "Payments Service can deliver the payment result."
            )
        try:
            result = await self._payments_client.create_payment(request)
        except PaymentServiceError:
            await self._cancel_order(order.id, reason="Payment service error")
            raise
        except Exception as e:  # любой сбой клиента = отмена заказа
            logger.warning("Payments client failure for order %s: %s", order.id, e)
            await self._cancel_order(order.id, reason=f"Payment failed: {e}")
            raise PaymentServiceError(
                f"Не удалось создать платёж для заказа {order.id}: {e}"
            ) from e

        async with self._uow() as uow:
            fresh = await uow.orders.get_by_id(order.id)
            if fresh is None:  # pragma: no cover - защита от гонки удаления
                raise PaymentServiceError(f"Заказ {order.id} исчез из хранилища.")

            if result.status is PaymentStatus.SUCCEEDED:
                fresh.mark_paid(payment_id=result.id)
            elif result.status is PaymentStatus.FAILED:
                fresh.mark_cancelled(reason="Payment rejected by service")
            else:  # PENDING — ждём callback
                fresh.payment_id = result.id

            updated = await uow.orders.update(fresh)
            logger.info(
                "Order %s: payment %s created, status=%s",
                order.id,
                result.id,
                updated.status.value,
            )
            return updated

    async def _cancel_order(self, order_id: str, reason: str) -> None:
        """Отменить заказ после сбоя платежа (best-effort)."""
        async with self._uow() as uow:
            order = await uow.orders.get_by_id(order_id)
            if order is None or order.status in (
                OrderStatus.CANCELLED,
                OrderStatus.SHIPPED,
            ):
                return
            order.mark_cancelled(reason=reason)
            await uow.orders.update(order)
            logger.info("Order %s cancelled: %s", order_id, reason)

    @staticmethod
    def _build_payment_idempotency_key(order_id: str) -> str:
        """Ключ идемпотентности платежа: жёстко привязан к order_id.

        Повторный вызов create_payment для того же заказа не создаст
        дубликат платежа в Payments Service (используется в Шаге 2).
        """
        return f"payment-{order_id}"
