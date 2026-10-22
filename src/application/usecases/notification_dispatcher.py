"""Центральный диспетчер уведомлений о статусах заказов.

Все use case'ы (создание заказа, callback платежа, события доставки)
используют ОДИН экземпляр OrderNotifier: фоновые задачи отправки
переживают завершение HTTP-запроса/обработки события, накапливаются в
одном месте и могут быть дожиданы при остановке приложения
(`wait_pending`), чтобы ни одно уведомление не потерялось.
"""

from src.application.ports.notifications import NotificationClient
from src.application.usecases.notify_order_status import OrderNotifier


class NotificationDispatcher:
    """Ленивый синглтон OrderNotifier для всего процесса."""

    def __init__(self, client: NotificationClient | None):
        self._client = client
        self._notifier: OrderNotifier | None = None

    @property
    def notifier(self) -> OrderNotifier:
        """Асинхронная отправка уведомлений об изменении статуса заказа."""
        if self._notifier is None:
            self._notifier = OrderNotifier(self._client)
        return self._notifier

    async def wait_pending(self) -> None:
        """Дождаться завершения всех отложенных отправок."""
        if self._notifier is not None:
            await self._notifier.wait_pending()
