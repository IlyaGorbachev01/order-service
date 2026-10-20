from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.infrastructure.persistence.database import get_engine
from src.presentation.api.routes import orders
from src.settings import get_settings

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Управление жизненным циклом приложения."""
    settings = get_settings()
    engine: AsyncEngine = get_engine(settings.database_url)

    yield

    # Очистка ресурсов при остановке (закрытие пула соединений)
    await engine.dispose()


def create_app() -> FastAPI:
    """Создает и настраивает экземпляр FastAPI."""
    app = FastAPI(
        title="Order Service",
        description="Сервис управления заказами Capashino",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(orders.router)

    return app


app = create_app()
