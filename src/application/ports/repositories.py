from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass

from src.domain.entities import Order, PaymentCallback
from src.domain.events import ShipmentReceivedEvent


@dataclass(frozen=True)
class OutboxRecord:
    """Запись таблицы Outbox, готовая к доставке (Kafka или Notifications).

    Value object уровня портов: инфраструктурная реализация репозитория
    отображает в него свои SQLAlchemy-модели, чтобы use cases и воркеры не
    зависели от деталей персистентности.

    Назначение записи определяет `event_type` (`OutboxEventType`):
    - `kafka_event`  — payload публикуется в Kafka-топик `topic`;
    - `notification` — payload разбирается в `NotificationCommand` и
      доставляется HTTP-клиентом Notifications Service (`topic` не
      используется, кладётся пустая строка).
    """

    id: int
    topic: str
    aggregate_id: str
    event_type: str
    payload: dict


class OrderRepository(ABC):
    """Абстрактный репозиторий для работы с заказами."""

    @abstractmethod
    async def get_by_id(self, order_id: str) -> Order | None:
        """Получить заказ по ID."""

    @abstractmethod
    async def get_by_idempotency_key(self, key: str) -> Order | None:
        """Получить заказ по ключу идемпотентности."""

    @abstractmethod
    async def create(self, order: Order) -> bool:
        """Создать новый заказ.

        Возвращает True, если заказ создан этим вызовом, и False, если
        заказ с таким `idempotency_key` уже существовал (идемпотентный
        повтор).
        """

    @abstractmethod
    async def update(self, order: Order) -> Order:
        """Сохранить изменения существующего заказа (статус, payment_id и т.п.)."""


class PaymentCallbackRepository(ABC):
    """Абстрактный репозиторий обработанных callback-ов Payments Service.

    Обеспечивает идемпотентность обработки callback-ов: факт обработки
    хранится в БД в рамках той же транзакции UoW, что и изменение заказа.
    """

    @abstractmethod
    async def exists(self, payment_id: str) -> bool:
        """Возвращает True, если callback с таким payment_id уже обработан."""

    @abstractmethod
    async def save(self, callback: PaymentCallback) -> None:
        """Записать факт обработки callback-а."""


class OutboxRepository(ABC):
    """Абстрактный репозиторий таблицы Outbox.

    События пишутся в ту же транзакцию UoW, что и изменение заказа, — так
    гарантируется атомарность «данные + событие». Отдельный воркер читает
    неотправленные записи и публикует их в Kafka (см. пункт 6 плана).
    """

    @abstractmethod
    async def add_event(
        self,
        *,
        aggregate_id: str,
        event_type: str,
        payload: dict,
        topic: str,
    ) -> None:
        """Поставить событие в очередь на публикацию (в текущей транзакции)."""

    @abstractmethod
    async def fetch_pending(self, limit: int = 50) -> Sequence[OutboxRecord]:
        """Выбрать неотправленные события для публикации.

        Возвращает записи `(id, topic, aggregate_id, event_type, payload)`;
        запись содержит идентификатор, чтобы воркер мог пометить её
        отправленной после успешной доставки в брокер.
        """

    @abstractmethod
    async def claim_pending(self, limit: int = 50) -> Sequence[OutboxRecord]:
        """Забрать партию событий с блокировкой строк (FOR UPDATE SKIP LOCKED).

        Гарантирует, что одна и та же запись не обрабатывается двумя
        экземплярами воркера одновременно: захвативший строку воркер
        публикует событие, затем помечает его `mark_published` в той же
        транзакции. Записи, заблокированные другими воркерами, пропускаются
        без ожидания. На СУБД без поддержки skip locked эквивалентен
        `fetch_pending`.
        """

    @abstractmethod
    async def mark_published(self, record_ids: Sequence[int]) -> None:
        """Пометить события отправленными (после успешной доставки в Kafka)."""


class InboxRepository(ABC):
    """Абстрактный репозиторий таблицы Inbox (дедупликация входящих событий)."""

    @abstractmethod
    async def is_processed(self, event_id: str) -> bool:
        """Возвращает True, если событие с таким `event_id` уже обработано."""

    @abstractmethod
    async def mark_processed(self, event: ShipmentReceivedEvent) -> None:
        """Записать факт обработки входящего события.

        Повторный вызов с тем же `event_id` нарушает уникальный индекс —
        защита от гонки между экземплярами консьюмера.
        """
