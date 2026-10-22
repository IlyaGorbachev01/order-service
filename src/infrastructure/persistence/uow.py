from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.ports.repositories import (
    InboxRepository,
    OrderRepository,
    OutboxRepository,
    PaymentCallbackRepository,
)
from src.application.ports.uow import BaseUnitOfWork
from src.infrastructure.persistence.repositories import (
    InboxRepositoryImpl,
    OrderRepositoryImpl,
    OutboxRepositoryImpl,
    PaymentCallbackRepositoryImpl,
)


class UnitOfWorkImpl(BaseUnitOfWork):
    """Реализация Unit of Work: заказы, callback-и, Outbox и Inbox."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory
        self._session: AsyncSession | None = None
        self._order_repo: OrderRepository | None = None
        self._payment_callback_repo: PaymentCallbackRepository | None = None
        self._outbox_repo: OutboxRepository | None = None
        self._inbox_repo: InboxRepository | None = None

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

    @property
    def payment_callbacks(self) -> PaymentCallbackRepository:
        """Возвращает репозиторий обработанных callback-ов Payments Service."""
        if self._payment_callback_repo is None:
            self._begin()
        assert self._payment_callback_repo is not None
        return self._payment_callback_repo

    @property
    def outbox(self) -> OutboxRepository:
        """Возвращает репозиторий исходящих событий (Outbox-паттерн)."""
        if self._outbox_repo is None:
            self._begin()
        assert self._outbox_repo is not None
        return self._outbox_repo

    @property
    def inbox(self) -> InboxRepository:
        """Возвращает репозиторий обработанных входящих событий (Inbox-паттерн)."""
        if self._inbox_repo is None:
            self._begin()
        assert self._inbox_repo is not None
        return self._inbox_repo

    def _begin(self) -> None:
        """Открывает сессию и инициализирует репозитории (идемпотентно)."""
        if self._session is None:
            self._session = self._session_factory()
            self._order_repo = OrderRepositoryImpl(self._session)
            self._payment_callback_repo = PaymentCallbackRepositoryImpl(self._session)
            self._outbox_repo = OutboxRepositoryImpl(self._session)
            self._inbox_repo = InboxRepositoryImpl(self._session)

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
        self._payment_callback_repo = None
        self._outbox_repo = None
        self._inbox_repo = None

    async def close(self) -> None:
        """Закрывает сессию без commit/rollback (для внешнего управления)."""
        if self._session:
            await self._session.close()
        self._session = None
        self._order_repo = None
        self._payment_callback_repo = None
        self._outbox_repo = None
        self._inbox_repo = None

    async def commit(self) -> None:
        """Фиксирует текущую транзакцию."""
        if self._session:
            await self._session.commit()

    async def rollback(self) -> None:
        """Откатывает текущую транзакцию."""
        if self._session:
            await self._session.rollback()
