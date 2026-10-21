from http import HTTPStatus

from fastapi import APIRouter, HTTPException

from src.application.usecases.create_order import CreateOrderCommand
from src.domain.exceptions import (
    CatalogUnavailableError,
    ConcurrentOrderCreationError,
    InsufficientStockError,
    OrderNotFoundError,
    PaymentServiceError,
)
from src.presentation.api.dependencies import CreateOrderUseCaseDep, GetOrderUseCaseDep
from src.presentation.api.schemas import OrderCreateRequest, OrderResponse

router = APIRouter(prefix="/api/orders", tags=["Orders"])


@router.post("", status_code=HTTPStatus.CREATED, response_model=OrderResponse)
async def create_order(
    request: OrderCreateRequest,
    use_case: CreateOrderUseCaseDep,
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
        order = await use_case(command)
        return OrderResponse.model_validate(order)
    except InsufficientStockError as e:
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
