from abc import ABC, abstractmethod
from collections.abc import Sequence

from src.domain.events import OrderPaidEvent


class EventPublisher(ABC):
    """Порт публикатора доменных событий в брокер (Kafka).

    Реализация находится в `infrastructure/messaging/`. UoW-слой использует
    этот порт для отправки уже сохранённых в Outbox событий: сам факт
    надёжного хранения события обеспечивает Outbox-таблица (см. пункт 4),
    а этот порт отвечает только за доставку «из таблицы в брокер».
    """

    @abstractmethod
    async def publish_order_paid(self, event: OrderPaidEvent) -> None:
        """Опубликовать событие `order.paid` в топик заказов.

        Используется как прямой (fire-and-forget) путь публикации; основной
        надёжный путь — пакетная отправка записей Outbox ниже.
        """

    @abstractmethod
    async def publish_raw(self, topic: str, key: str | None, payload: dict) -> None:
        """Опубликовать произвольный JSON-событие в указанный топик.

        Args:
            topic: имя Kafka-топика (например, `student_system-order.events`).
            key: ключ партиционирования (обычно `order_id`) или None.
            payload: сериализуемый в JSON словарь события.
        """

    @abstractmethod
    async def publish_batch(
        self,
        records: Sequence[tuple[str, str | None, dict]],
    ) -> int:
        """Опубликовать пакет записей Outbox и вернуть число успешно отправленных.

        Args:
            records: последовательности кортежей `(topic, key, payload)`.

        Returns:
            Количество успешно опубликованных событий. При сбое доставки
            брокером должно быть выброшено исключение — вызывающий код
            (outbox-воркер) тогда не помечает записи отправленными.
        """
