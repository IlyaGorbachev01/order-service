from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Self

from src.application.ports.repositories import (
    OrderRepository,
    PaymentCallbackRepository,
)


class UnitOfWork(ABC):
    """Единая точка работы (Unit of Work).

    Контракт транзакционной границы приложения.
    """

    @property
    @abstractmethod
    def orders(self) -> OrderRepository:
        """Репозиторий заказов."""

    @property
    @abstractmethod
    def payment_callbacks(self) -> PaymentCallbackRepository:
        """Репозиторий обработанных callback-ов Payments Service."""

    @abstractmethod
    async def commit(self) -> None:
        """Зафиксировать транзакцию."""


class BaseUnitOfWork(UnitOfWork):
    """Базовая реализация для использования в виде контекстного менеджера."""

    @asynccontextmanager
    async def __call__(self) -> AsyncGenerator[Self]:
        """Войти в транзакционную единицу работы как в асинхронный контекстный менеджер.

        При успешном выходе из контекста фиксирует транзакцию (commit).
        Если внутри контекста возникло исключение — откатывает изменения
        (rollback) и пробрасывает исключение дальше.

        Yields:
            Сам объект UoW, через который доступны репозитории
            (например, `orders`), работающие в рамках текущей транзакции.

        Raises:
            Exception: Пробрасывает любое исключение, возникшее
                в теле контекста, после выполнения rollback.
        """
        try:
            yield self
            await self.commit()
        except Exception:
            await self.rollback()
            raise

    @abstractmethod
    async def rollback(self) -> None:
        """Откатить транзакцию."""
