from abc import ABC, abstractmethod

from src.domain.events import ShipmentReceivedEvent


class InboxStorage(ABC):
    """Порт Inbox-паттерна: дедупликация входящих событий из Kafka.

    Гарантивает идемпотентность обработки событий от Shipping Service:
    событие считается обработанным не более одного раза, даже если брокер
    доставил его повторно (at-least-once семантика Kafka).

    Контракт «проверил-и-записал атомарно» реализует вызывающий код внутри
    одной транзакции UoW (`is_processed` + `mark_processed` в одном коммите),
    поэтому сам порт не предоставляет «upsert»-метод — только чтение и запись.
    """

    @abstractmethod
    async def is_processed(self, event_id: str) -> bool:
        """Возвращает True, если событие с таким `event_id` уже обработано."""

    @abstractmethod
    async def mark_processed(self, event: ShipmentReceivedEvent) -> None:
        """Записать факт обработки события.

        Повторный вызов с тем же `event_id` должен нарушать уникальность
        (уникальный индекс по `event_id`) — это защита от гонки между
        двумя экземплярами консьюмера, читающими одну партицию.
        """
