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
        """Возвращает репозиторий заказов.

        При первом обращении вне открытой транзакции лениво открывает
        сессию, чтобы UoW можно было использовать и напрямую
        (`uow.orders`), и как контекстный менеджер (`async with uow():`).
        """
        if self._order_repo is None:
            self._begin()
        assert self._order_repo is not None
        return self._order_repo

    def _begin(self) -> None:
        """Открывает сессию и инициализирует репозитории (идемпотентно)."""
        if self._session is None:
            self._session = self._session_factory()
            self._order_repo = OrderRepositoryImpl(self._session)

    async def __aenter__(self) -> "UnitOfWorkImpl":
        """Открывает сессию и инициализирует репозитории."""
        self._begin()
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

    async def close(self) -> None:
        """Закрывает сессию без commit/rollback (для внешнего управления)."""
        if self._session:
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
