"""Kafka-инфраструктура Order Service.

Два компонента:

* :class:`KafkaEventPublisher` — реализация порта `EventPublisher`;
  используется outbox-воркером для доставки событий из таблицы Outbox
  в топик `student_system-order.events`.
* :class:`ShipmentConsumer` — фоновый консьюмер топика
  `student_system-shipment.events`: читает события Shipping Service,
  строит доменное событие и передаёт его use case (Inbox-идемпотентность
  обеспечивает сам use case).

Оба компонента живут в отдельном фоновом процессе
(`python -m src.bin.worker`, см. `run-worker.sh`), а не внутри API.
"""

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable, Coroutine, Sequence

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord

from src.application.ports.event_publisher import EventPublisher
from src.domain.events import OrderPaidEvent, ShipmentReceivedEvent
from src.domain.exceptions import OrderNotFoundError
from src.settings import Settings

logger = logging.getLogger(__name__)

# Обработчик входящего события, предоставляемый слоем сборки (presentation):
# async def handler(event: ShipmentReceivedEvent) -> None
EventHandler = Callable[[ShipmentReceivedEvent], Coroutine[None, None, None]]


class KafkaEventPublisher(EventPublisher):
    """Реализация порта EventPublisher поверх aiokafka.

    Один продюсер переиспользуется всеми воркерами (соединение с брокером
    дорогое). Публикация подтверждённая (`acks=all`): исключение
    бросается только когда доставка гарантированно не состоялась, —
    outbox-воркер в этом случае не помечает записи отправленными.
    """

    def __init__(self, settings: Settings):
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Подключиться к брокеру (вызывается при старте worker-процесса)."""
        if self._producer is not None:
            return
        producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                "utf-8"
            ),
            acks="all",
            enable_idempotence=True,
            linger_ms=50,
        )
        await producer.start()
        self._producer = producer
        logger.info(
            "Kafka publisher started (bootstrap=%s)",
            self._settings.kafka_bootstrap_servers,
        )

    async def stop(self) -> None:
        """Завершить работу продюсера (дожидаться отправки буфера)."""
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
            logger.info("Kafka publisher stopped")

    @property
    def _ready(self) -> AIOKafkaProducer:
        if self._producer is None:
            raise RuntimeError("KafkaEventPublisher не запущен (start() не вызван)")
        return self._producer

    async def publish_raw(self, topic: str, key: str | None, payload: dict) -> None:
        """Опубликовать одно JSON-событие в указанный топик."""
        await self._ready.send_and_wait(topic, value=payload, key=key)

    async def publish_order_paid(self, event: OrderPaidEvent) -> None:
        """Прямая (fire-and-forget) публикация order.paid в топик заказов."""
        await self.publish_raw(
            self._settings.order_events_topic,
            key=event.order_id,
            payload=event.to_dict(),
        )

    async def publish_batch(
        self,
        records: Sequence[tuple[str, str | None, dict]],
    ) -> int:
        """Опубликовать пакет записей Outbox.

        При сбое доставки бросает исключение — вызывающий код (outbox-
        воркер) тогда не помечает записи отправленными, и они будут
        повторены позже (at-least-once; дедупликация на стороне подписчика).
        """
        producer = self._ready
        published = 0
        for topic, key, payload in records:
            await producer.send_and_wait(topic, value=payload, key=key)
            published += 1
        return published


class ShipmentConsumer:
    """Фоновый консьюмер событий Shipping Service.

    Читает топик `student_system-shipment.events` и передаёт каждое
    событие обработчику (use case с Inbox-паттерном). Ошибки обработки
    логируются, но не останавливают консьюмер: семантика at-least-once
    плюс идемпотентный Inbox делают повторную доставку безопасной.
    """

    def __init__(
        self,
        settings: Settings,
        handler: EventHandler,
        *,
        retry_interval: float = 5.0,
    ):
        self._settings = settings
        self._handler = handler
        self._retry_interval = retry_interval
        self._task: asyncio.Task | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._stopping = False

    async def start(self) -> None:
        """Запустить фоновую задачу потребления (не блокирует старт приложения)."""
        if self._task is not None:
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="shipment-consumer")
        logger.info(
            "Shipment consumer started (topic=%s, group=%s)",
            self._settings.shipment_events_topic,
            self._settings.kafka_consumer_group_id,
        )

    async def stop(self) -> None:
        """Остановить консьюмер и дождаться завершения фоновой задачи."""
        self._stopping = True
        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except Exception:
                logger.exception("Error while stopping Kafka consumer")
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        logger.info("Shipment consumer stopped")

    async def _run(self) -> None:
        """Цикл с переподключением: сбой брокера не должен убивать консьюмер."""
        while not self._stopping:
            try:
                await self._consume_loop()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Shipment consumer crashed, reconnecting in %.1fs",
                    self._retry_interval,
                )
                await asyncio.sleep(self._retry_interval)

    async def _consume_loop(self) -> None:
        consumer = AIOKafkaConsumer(
            self._settings.shipment_events_topic,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=self._settings.kafka_consumer_group_id,
            # Стратегия чтения с бордера (покрыто тестами автотеста):
            # * `earliest` — консьюмер стартует с начала топика и не
            #   пропускает события, опубликованные до его подключения
            #   (с `latest` order.shipped, пришедший за секунды до старта
            #   воркера, терялся бы навсегда);
            #   старые/повторные события безопасно отсекаются Inbox'ом.
            # * авто-коммит только по успешной обработке (`commit()` после
            #   handler'а), периодический авто-коммит выключен — при сбое
            #   обработки offset не уходит вперёд и сообщение будет
            #   повторено (at-least-once + идемпотентный Inbox).
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            isolation_level="read_committed",
        )
        await consumer.start()
        self._consumer = consumer
        logger.info(
            "Shipment consumer connected to %s",
            self._settings.kafka_bootstrap_servers,
        )
        try:
            async for message in consumer:
                await self._handle_message(message)
                # Manual commit после успешной обработки (или штатного
                # пропуска дубликата): при исключении обработчика offset
                # не двигается и Kafka повторит доставку — at-least-once.
                await consumer.commit()
        finally:
            await consumer.stop()
            self._consumer = None

    async def _handle_message(self, message: ConsumerRecord) -> None:
        """Обработать одно Kafka-сообщение.

        Возвращается штатно при успешной обработке или идемпотентном
        пропуске (дубликат в Inbox, недопустимый переход — use case гасит
        их сам). Бросает исключение только если заказ ещё не виден в БД
        (`OrderNotFoundError` — например, `order.shipped` пришёл раньше,
        чем завершилась транзакция создания заказа): консьюмер тогда НЕ
        коммитит offset, и брокер повторит доставку позже.
        """
        try:
            payload = json.loads(message.value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.error(
                "Malformed shipment event at %s-%s@%s, skipping",
                message.topic,
                message.partition,
                message.offset,
            )
            return

        fallback_id = f"kafka-{message.topic}-{message.partition}-{message.offset}"
        event = ShipmentReceivedEvent.from_payload(
            payload,
            fallback_event_id=fallback_id,
        )

        try:
            await self._handler(event)
        except OrderNotFoundError as e:
            # Заказ ещё не зафиксирован — не подтверждаем offset, Kafka
            # доставит сообщение заново (at-least-once + Inbox безопасны).
            logger.warning(
                "Shipment event %s for unknown order %s: not committing "
                "offset, retry will follow: %s",
                event.event_id,
                event.order_id,
                e,
            )
            raise
        except Exception:
            # Прочие сбои обработки логируем и подтверждаем offset, чтобы
            # не зациклить консьюмер на «битом» сообщении; дубликаты и
            # гонки уже обработаны Inbox-идемпотентностью.
            logger.exception(
                "Failed to handle shipment event %s (type=%s, order=%s)",
                event.event_id,
                event.event_type,
                event.order_id,
            )
