from functools import lru_cache
from typing import Annotated

from fastapi import Depends

from src.application.usecases.create_order import CreateOrderUseCase
from src.application.usecases.get_order import GetOrderUseCase
from src.infrastructure.http.catalog import CatalogClientImpl
from src.infrastructure.persistence.database import get_session_factory
from src.infrastructure.persistence.uow import UnitOfWorkImpl
from src.settings import Settings, get_settings


@lru_cache
def _get_settings() -> Settings:
    """Кэширует настройки приложения (читаются один раз при старте)."""
    return get_settings()


SettingsDep = Annotated[Settings, Depends(_get_settings)]


def _get_uow(settings: SettingsDep) -> UnitOfWorkImpl:
    """Создает Unit of Work с фабрикой сессий."""
    session_factory = get_session_factory(settings.database_url)
    return UnitOfWorkImpl(session_factory)


def _get_catalog_client(settings: SettingsDep) -> CatalogClientImpl:
    """Создает клиент каталога."""
    return CatalogClientImpl(settings)


# Зависимости уровня приложения (синглтоны на весь жизненный цикл)
UowDep = Annotated[UnitOfWorkImpl, Depends(_get_uow)]
CatalogClientDep = Annotated[CatalogClientImpl, Depends(_get_catalog_client)]


# --- Фабрики use case'ов ---


def get_create_order_use_case(
    uow: UowDep,
    catalog_client: CatalogClientDep,
) -> CreateOrderUseCase:
    """Создает сценарий оформления заказа."""
    return CreateOrderUseCase(uow, catalog_client)


def get_get_order_use_case(
    uow: UowDep,
) -> GetOrderUseCase:
    """Создает сценарий получения заказа."""
    return GetOrderUseCase(uow)


CreateOrderUseCaseDep = Annotated[
    CreateOrderUseCase, Depends(get_create_order_use_case)
]
GetOrderUseCaseDep = Annotated[GetOrderUseCase, Depends(get_get_order_use_case)]
