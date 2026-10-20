from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def get_engine(database_url: str) -> AsyncEngine:
    """Создает и возвращает асинхронный движок SQLAlchemy."""
    return create_async_engine(
        database_url,
        echo=False,
        future=True,
        pool_pre_ping=True,
    )


def get_session_factory(database_url: str) -> async_sessionmaker[AsyncSession]:
    """Создает фабрику асинхронных сессий."""
    engine = get_engine(database_url)
    return async_sessionmaker(
        engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )
