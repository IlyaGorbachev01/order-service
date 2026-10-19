from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.ports.repositories import OrderRepository
from src.application.ports.uow import BaseUnitOfWork
from src.infrastructure.persistence.repositories import OrderRepositoryImpl


class UnitOfWorkImpl(BaseUnitOfWork):
    """Реализация Unit of Work для работы с заказами."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory
        self._session: AsyncSession | None = None
        self._order_repo: OrderRepository | None = None

    @property
    def orders(self) -> OrderRepository:
        """Возвращает репозиторий заказов."""
        if self._order_repo is None or self._session is None:
            raise RuntimeError(
                "UnitOfWork не инициализирован."
                "Используйте его как контекстный менеджер."
            )
        return self._order_repo

    async def __aenter__(self) -> "UnitOfWorkImpl":
        """Открывает сессию и инициализирует репозитории."""
        self._session = self._session_factory()
        self._order_repo = OrderRepositoryImpl(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Откатывает транзакцию при ошибке и закрывает сессию."""
        if self._session:
            if exc_type is not None:
                await self._session.rollback()
            await self._session.close()
        self._session = None
        self._order_repo = None

    async def commit(self) -> None:
        """Фиксирует текущую транзакцию."""
        if self._session:
            await self._session.commit()

    async def rollback(self) -> None:
        """Откатывает текущую транзакцию."""
        if self._session:
            await self._session.rollback()
