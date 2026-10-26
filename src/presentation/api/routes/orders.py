from http import HTTPStatus

from fastapi import APIRouter, BackgroundTasks, HTTPException

from src.application.usecases.create_order import CreateOrderCommand, NotificationPort
from src.domain.exceptions import (
    CatalogUnavailableError,
    ConcurrentOrderCreationError,
    InsufficientStockError,
    ItemNotFoundError,
    OrderNotFoundError,
    PaymentServiceError,
)
from src.presentation.api.dependencies import (
    CreateOrderUseCaseDep,
    GetOrderUseCaseDep,
    NotificationDispatcherDep,
)
from src.presentation.api.schemas import OrderCreateRequest, OrderResponse

router = APIRouter(prefix="/api/orders", tags=["Orders"])


async def _notify_new_order(
    notifier: NotificationPort, order: object, created: bool
) -> None:
    """Фоновая задача: уведомить о NEW только если заказ создан этим запросом."""
    if created:
        await notifier.notify(order)  # type: ignore[arg-type]


@router.post("", status_code=HTTPStatus.CREATED, response_model=OrderResponse)
async def create_order(
    request: OrderCreateRequest,
    background_tasks: BackgroundTasks,
    use_case: CreateOrderUseCaseDep,
    dispatcher: NotificationDispatcherDep,
) -> OrderResponse:
    """Создает новый заказ.

    Идемпотентность: повторный запрос с тем же idempotency_key возвращает
    тот же заказ (201). Если параллельный запрос не успел зафиксировать
    заказ — отдаём 503 (клиент/тест может повторить запрос), но не 500.
    """
    try:
        command = CreateOrderCommand(
            user_id=request.user_id,
            quantity=request.quantity,
            item_id=request.item_id,
            idempotency_key=request.idempotency_key,
        )
        order, created = await use_case(command)
        # Уведомление о создании заказа (NEW) — в фоне, после ответа клиенту.
        # Для повторного идемпотентного запроса (created=False) задача
        # no-op: уведомление не дублируется (плюс детерминированный ключ
        # {order_id}:{status} на стороне Notifications Service).
        notifier: NotificationPort = dispatcher.request_notifier(
            background_tasks.add_task
        )
        background_tasks.add_task(_notify_new_order, notifier, order, created)
        return OrderResponse.model_validate(order)
    except (InsufficientStockError, ItemNotFoundError) as e:
        # Товар отсутствует или его недостаточно — бизнес-ошибка клиента.
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e)) from e
    except CatalogUnavailableError as e:
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=str(e)
        ) from e
    except ConcurrentOrderCreationError as e:
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail=(
                "Заказ с таким idempotency_key создаётся параллельным запросом, "
                "повторите попытку."
            ),
        ) from e
    except PaymentServiceError as e:
        # Заказ создан, но платёж не прошёл — заказ отменён (CANCELLED),
        # для клиента это ошибка зависимого сервиса.
        raise HTTPException(status_code=HTTPStatus.BAD_GATEWAY, detail=str(e)) from e


@router.get("/{order_id}", status_code=HTTPStatus.OK, response_model=OrderResponse)
async def get_order(
    order_id: str,
    use_case: GetOrderUseCaseDep,
) -> OrderResponse:
    """Возвращает заказ по идентификатору."""
    try:
        order = await use_case(order_id)
        return OrderResponse.model_validate(order)
    except OrderNotFoundError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
