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


@lru_cache
def _get_session_factory(settings: Settings):  # noqa: ANN202
    """Кэширует фабрику сессий (один движок на весь жизненный цикл)."""
    return get_session_factory(settings.database_url)


def _get_uow(settings: SettingsDep) -> UnitOfWorkImpl:
    """Создает Unit of Work с кэшированной фабрикой сессий (на каждый запрос)."""
    return UnitOfWorkImpl(_get_session_factory(settings))


_catalog_client_singleton: CatalogClientImpl | None = None


def _get_catalog_client(settings: SettingsDep) -> CatalogClientImpl:
    """Возвращает синглтон клиента каталога (создается при первом запросе)."""
    global _catalog_client_singleton
    if _catalog_client_singleton is None:
        _catalog_client_singleton = CatalogClientImpl(settings)
    return _catalog_client_singleton


# Зависимости уровня запроса / приложения
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


async def shutdown_resources() -> None:
    """Закрывает ресурсы инфраструктуры при остановке приложения."""
    global _catalog_client_singleton
    if _catalog_client_singleton is not None:
        await _catalog_client_singleton.close()
        _catalog_client_singleton = None
