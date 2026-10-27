import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from src.application.ports.catalog import CatalogClient, CatalogItem
from src.application.ports.payments import PaymentRequest, PaymentsClient
from src.application.ports.uow import BaseUnitOfWork
from src.application.usecases.notify_order_status import enqueue_status_notification
from src.domain.entities import Order, OrderStatus, PaymentStatus
from src.domain.exceptions import (
    CatalogUnavailableError,
    ConcurrentOrderCreationError,
    InsufficientStockError,
    InvalidOrderTransitionError,
    ItemNotFoundError,
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
        # Уведомления НЕ внедряются в виде клиента: use case пишет запись
        # NOTIFICATION в outbox (`uow.outbox`) в той же транзакции, что и
        # смену статуса — at-least-once доставка через единый механизм
        # (см. notify_order_status.enqueue_status_notification).

    async def __call__(self, command: CreateOrderCommand) -> tuple[Order, bool]:
        """Выполнить сценарий создания заказа.

        Возвращает (заказ, создан_сейчас). Флаг нужен HTTP-слою, чтобы
        отправлять уведомление о NEW ровно один раз: повторный идемпотентный
        запрос возвращает существующий заказ без побочных эффектов.
        """
        order, created = await self._create_order(command)
        if created and self._payments_client is not None:
            # Платёж создаём только для нового заказа: повторный запрос с тем же
            # idempotency_key возвращает исходный заказ без побочных эффектов.
            try:
                amount = await self._resolve_amount(order)
                order = await self._create_payment(order, amount, self._callback_url)
            except PaymentServiceError as e:
                # Платёжный сбой — побочный эффект, а не отказ валидации:
                # заказ уже сохранён и отменён (CANCELLED), основной ответ
                # остаётся успешным 201 по спецификации. Иначе недоступный
                # Payments превращал happy-path в 5xx (провалившийся шаг
                # автотеста order_create_success). Причина логируется и
                # видна в Sentry как handled-исключение.
                logger.warning(
                    "Order %s created but payment failed; returning 201 "
                    "with cancelled order: %s",
                    order.id,
                    e,
                )
                order = await self._reload_order(order.id) or order
            except (ItemNotFoundError, CatalogUnavailableError) as e:
                # Товар «исчез» из каталога между проверкой стока и
                # расчётом суммы либо Catalog Service стал недоступен.
                # Это тоже сбой зависимого сервиса на этапе платежа:
                # отменяем заказ и возвращаем 201 с CANCELLED — тест
                # может повторить запрос с новым idempotency_key.
                logger.warning(
                    "Order %s: catalog failure during payment step (%s); "
                    "cancelling order and returning 201.",
                    order.id,
                    e,
                )
                await self._cancel_order(
                    order.id, reason=f"Catalog failure at payment step: {e}"
                )
                order = await self._reload_order(order.id) or order
        return order, created

    async def _reload_order(self, order_id: str) -> Order | None:
        """Прочитать актуальное состояние заказа (для ответа после сбоя)."""
        try:
            async with self._uow() as uow:
                return await uow.orders.get_by_id(order_id)
        except Exception:  # pragma: no cover - best-effort перечитывание
            logger.exception("Failed to reload order %s", order_id)
            return None

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

        Важно: транзакция фиксируется (`commit`) до выхода из блока, а не
        только на `__aexit__` UoW. Иначе вызовы внешних сервисов (Catalog
        при расчёте суммы, Payments при создании платежа, Notifications
        при отправке уведомления о NEW) попадали бы в окно между началом
        транзакции и её коммитом: Payments Service мог доставить callback
        ОБРАТНО в этот же сервис ещё до коммита заказа. Callback работает
        в отдельном соединении БД и при READ COMMITTED не видит незаком-
        миченный заказ -> 404 OrderNotFound; повторной доставки Payments
        Service не делает, заказ навсегда остаётся в NEW, а тест не дос-
        тигает ни PAID, ни SHIPPED (провалившийся шаг автотеста).
        Явный commit сразу после create() устраняет это состояние гонки:
        к моменту HTTP-вызова в Payments заказ уже виден всем соединениям.
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

            # 4. Сохранение заказа в статусе NEW + уведомление о NEW — ОДНОЙ
            #    транзакцией (атомарно: либо заказ создан и уведомление
            #    поставлено в очередь, либо откат обоих). Затем явный коммит
            #    (см. docstring).
            #    Нарушение unique по idempotency_key бросает
            #    ConcurrentOrderCreationError -> повтор в _create_order.
            created = await uow.orders.create(order)
            if created:
                # Уведомление о создании (NEW) — через transactional outbox.
                # Для повторного идемпотентного запроса (created=False)
                # уведомление не ставится повторно.
                await enqueue_status_notification(uow.outbox, order)
            await uow.commit()
            return order, created

    async def _find_existing(self, idempotency_key: str) -> Order | None:
        """Прочитать заказ, созданный конкурентным запросом."""
        async with self._uow() as uow:
            return await uow.orders.get_by_idempotency_key(idempotency_key)

    async def _get_catalog_item(self, item_id: str) -> CatalogItem:
        """Получить товар из Catalog Service.

        Клиент каталога сам транслирует HTTP-детали в доменные исключения
        (`ItemNotFoundError` для 404, `CatalogUnavailableError` для прочих
        сбоев) — здесь нет импортов httpx (правило зависимостей). Роут
        превращает их в 400/422, но не в 500 Internal Server Error.
        """
        return await self._catalog_client.get_item(item_id)

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

        Важно про порядок операций: HTTP-запрос к Payments Service
        выполняется ДО открытия транзакции перечитывания заказа. UoW
        открывает сессию лениво при первом обращении к репозиторию, а
        SQLAlchemy начинает SQL-транзакцию (`BEGIN`) на первом же запросе.
        Если бы чтение `fresh` предшествовало вызову платежей, к моменту
        HTTP-запроса у процесса уже висела бы открытая (в READ COMMITTED —
        snapshot для REPEATABLE READ) транзакция. Асинхронный Payments
        Service доставляет callback ОБРАТНО в этот же сервис, callback
        коммитит переход заказа в PAID в другом соединении, а «старое»
        зависшее транзакт-чтение затем перезаписывало бы статус своим
        устаревшим снимком (NEW) — платеж терялся, заказ залипал в NEW,
        событие order.paid не публиковалось и доставка никогда не
        происходила (провалившийся шаг автотеста). Поэтому сначала
        внешний вызов, потом свежая транзакция перечитывания.

        Идемпотентность перехода в PAID: Payments Service асинхронен и
        может доставить успешный callback ОБРАТНО раньше, чем вернётся
        HTTP-ответ create_payment. Тогда заказ в БД уже PAID, а повторный
        `mark_paid` запрещён машиной состояний (PAID -> PAID) и бросил бы
        InvalidOrderTransitionError -> 502 + отмена уже оплаченного заказа.
        Это штатная гонка: событие order.paid уже поставлено в Outbox
        callback-ом, поэтому просто оставляем текущий статус.
        """
        request = PaymentRequest(
            order_id=order.id,
            amount=amount.quantize(Decimal("0.01")),
            callback_url=callback_url,
            idempotency_key=self._build_payment_idempotency_key(order.id),
        )
        # Платёжный клиент обязателен: __call__ вызывает _create_payment
        # только когда self._payments_client is not None.
        assert self._payments_client is not None
        if not callback_url:
            logger.warning(
                "callback_url is empty: set ORDER_SERVICE_INTERNAL_URL env var "
                "(k8s internal host <service>.<namespace>.svc:<port>) so that "
                "Payments Service can deliver the payment result."
            )
        try:
            result = await self._payments_client.create_payment(request)
        except PaymentServiceError as e:
            # Бросаем исходную ошибку ПОСЛЕ отмены заказа: _cancel_order
            # использует новый UoW (новую сессию/транзакцию), а в SQLite
            # пул StaticPool разделяет одно соединение между сессиями —
            # открытый к этому моменту read-only транзакт перечитывания
            # заказа блокировал бы запись отмены и давал "database is
            # locked" вместо честного PaymentServiceError.
            await self._cancel_order(order.id, reason="Payment service error")
            raise e
        except Exception as e:  # любой сбой клиента = отмена заказа
            logger.warning("Payments client failure for order %s: %s", order.id, e)
            await self._cancel_order(order.id, reason=f"Payment failed: {e}")
            raise PaymentServiceError(
                f"Не удалось создать платёж для заказа {order.id}: {e}"
            ) from e

        # Перечитывание и актуализация заказа — отдельная (новая)
        # транзакция, открытая уже ПОСЛЕ внешнего вызова (см. docstring).
        # Любые сбои этого шага означают «платёж не зафиксирован в
        # заказе»: заказ отменяется, ошибка отдаётся наружу.
        try:
            async with self._uow() as uow:
                fresh = await uow.orders.get_by_id(order.id)
                if fresh is None:  # pragma: no cover - защита от гонки удаления
                    raise PaymentServiceError(f"Заказ {order.id} исчез из хранилища.")

                if result.status is PaymentStatus.SUCCEEDED:
                    try:
                        fresh.mark_paid(payment_id=result.id)
                    except InvalidOrderTransitionError:
                        # Гонка с callback: платёжный сервис доставил успех
                        # раньше нашего HTTP-ответа — заказ уже PAID/SHIPPED,
                        # событие order.paid уже в Outbox. Повторный переход
                        # не требуется; прочее (CANCELLED) — честная ошибка.
                        if fresh.status in (OrderStatus.PAID, OrderStatus.SHIPPED):
                            logger.info(
                                "Order %s already %s via payment callback; "
                                "skipping duplicate mark_paid (payment %s)",
                                fresh.id,
                                fresh.status.value,
                                result.id,
                            )
                        else:
                            raise
                elif result.status is PaymentStatus.FAILED:
                    fresh.mark_cancelled(reason="Payment rejected by service")
                else:  # PENDING — ждём callback
                    fresh.payment_id = result.id

                updated = await uow.orders.update(fresh)
                await uow.commit()
                logger.info(
                    "Order %s: payment %s created, status=%s",
                    order.id,
                    result.id,
                    updated.status.value,
                )
                # Уведомление о синхронном итоге платежа (PAID/CANCELLED) —
                # в ту же транзакцию, что и update() заказа (атомарно).
                # Для PENDING итог придёт на callback и будет поставлен там.
                if updated.status in (OrderStatus.PAID, OrderStatus.CANCELLED):
                    await enqueue_status_notification(uow.outbox, updated)
                return updated
        except PaymentServiceError:
            raise
        except Exception as e:
            logger.exception(
                "Failed to register payment %s for order %s", result.id, order.id
            )
            await self._cancel_order(
                order.id, reason=f"Payment registration failed: {e}"
            )
            raise PaymentServiceError(
                f"Не удалось зафиксировать платёж для заказа {order.id}: {e}"
            ) from e

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
            updated = await uow.orders.update(order)
            # Уведомление об отмене (CANCELLED) — в ту же транзакцию, что и
            # смена статуса: атомарно, доставка через outbox-воркер.
            await enqueue_status_notification(uow.outbox, updated)
            await uow.commit()
            logger.info("Order %s cancelled: %s", order_id, reason)

    @staticmethod
    def _build_payment_idempotency_key(order_id: str) -> str:
        """Ключ идемпотентности платежа: жёстко привязан к order_id.

        Повторный вызов create_payment для того же заказа не создаст
        дубликат платежа в Payments Service (используется в Шаге 2).
        """
        return f"payment-{order_id}"
