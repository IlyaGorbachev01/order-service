from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class OrderCreateRequest(BaseModel):
    """Схема запроса на создание заказа."""

    user_id: str = Field(..., description="ID пользователя")
    quantity: int = Field(..., gt=0, description="Количество товара")
    item_id: str = Field(..., description="ID товара")
    idempotency_key: str = Field(..., description="Ключ идемпотентности")


class OrderResponse(BaseModel):
    """Схема ответа с данными заказа."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: str
    item_id: str
    quantity: int
    status: str
    created_at: datetime
    updated_at: datetime
