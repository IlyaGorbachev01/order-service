import logging
from http import HTTPStatus

from fastapi import APIRouter, HTTPException

from src.domain.entities import PaymentCallback, PaymentStatus
from src.domain.exceptions import OrderNotFoundError
from src.presentation.api.dependencies import ProcessPaymentCallbackUseCaseDep
from src.presentation.api.schemas import PaymentCallbackRequest

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/orders", tags=["Callbacks"])


@router.post("/payment-callback", status_code=HTTPStatus.OK)
async def payment_callback(
    request: PaymentCallbackRequest,
    use_case: ProcessPaymentCallbackUseCaseDep,
) -> dict[str, str]:
    """Принимает callback от Payments Service о результате платежа.

    Всегда отвечает 200 OK для корректно сформированного запроса,
    включая повторные (дубликаты) callback-и — обработка идемпотентна.
    """
    try:
        status = PaymentStatus(request.status)
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Неизвестный статус платежа: {request.status}",
        ) from e

    callback = PaymentCallback(
        payment_id=request.payment_id,
        order_id=request.order_id,
        status=status,
        amount=request.amount,
        error_message=request.error_message,
    )

    try:
        await use_case(callback)
    except OrderNotFoundError as e:
        # Заказ не найден — отвечаем 404, чтобы источник мог ретраить/алертить
        logger.warning("Callback for unknown order: %s", e)
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e

    return {"status": "ok"}
