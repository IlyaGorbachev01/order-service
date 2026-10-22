import logging
from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.usecases.create_order import CreateOrderUseCase
from src.application.usecases.get_order import GetOrderUseCase
from src.application.usecases.handle_shipment_event import HandleShipmentEventUseCase
from src.application.usecases.notification_dispatcher import NotificationDispatcher
from src.application.usecases.notify_order_status import OrderNotifier
from src.application.usecases.process_payment_callback import (
    ProcessPaymentCallbackUseCase,
)
from src.domain.events import ShipmentReceivedEvent
from src.infrastructure.http.catalog import CatalogClientImpl
from src.infrastructure.http.notifications import NotificationsClientImpl
from src.infrastructure.http.payments import PaymentsClientImpl
from src.infrastructure.messaging.kafka import KafkaEventPublisher, ShipmentConsumer
from src.infrastructure.messaging.outbox_worker import OutboxWorker
from src.infrastructure.persistence.database import get_session_factory
from src.infrastructure.persistence.uow import UnitOfWorkImpl
from src.settings import Settings, get_settings

logger = logging.getLogger(__name__)


@lru_cache
def _get_settings() -> Settings:
    """Кэширует настройки приложения (читаются один раз при старте)."""
    return get_settings()


SettingsDep = Annotated[Settings, Depends(_get_settings)]


_session_factory_singleton = None


def _get_session_factory(settings: Settings) -> async_sessionmaker[AsyncSession]:
    """Кэширует фабрику сессий (один движок на весь жизненный цикл).

    Примечание: lru_cache здесь не используется, т.к. pydantic-модель
    Settings не хешируется (unhashable type: 'Settings').
    """
    global _session_factory_singleton
    if _session_factory_singleton is None:
        _session_factory_singleton = get_session_factory(settings.database_url)
    return _session_factory_singleton


def _get_uow(settings: SettingsDep) -> UnitOfWorkImpl:
    """Создает Unit of Work с кэшированной фабрикой сессий (на каждый запрос)."""
    return UnitOfWorkImpl(_get_session_factory(settings))


_catalog_client_singleton: CatalogClientImpl | None = None


def _get_catalog_client(settings: SettingsDep) -> CatalogClientImpl:
    """Возвращает синглтон клиента каталога (создается при первом запросе)."""
    global _catalog_client_singleton
    if _catalog_client_singleton is None:
        _catalog_client_singleton = CatalogClientImpl(settings)
    return _catalog_client_singleton


_payments_client_singleton: PaymentsClientImpl | None = None


def _get_payments_client(settings: SettingsDep) -> PaymentsClientImpl:
    """Возвращает синглтон клиента Payments Service."""
    global _payments_client_singleton
    if _payments_client_singleton is None:
        _payments_client_singleton = PaymentsClientImpl(settings)
    return _payments_client_singleton


_notifications_client_singleton: NotificationsClientImpl | None = None


def _get_notifications_client(settings: SettingsDep) -> NotificationsClientImpl:
    """Возвращает синглтон клиента Notifications Service (Шаг 4)."""
    global _notifications_client_singleton
    if _notifications_client_singleton is None:
        _notifications_client_singleton = NotificationsClientImpl(settings)
    return _notifications_client_singleton


# Зависимости уровня запроса / приложения
UowDep = Annotated[UnitOfWorkImpl, Depends(_get_uow)]
CatalogClientDep = Annotated[CatalogClientImpl, Depends(_get_catalog_client)]
PaymentsClientDep = Annotated[PaymentsClientImpl, Depends(_get_payments_client)]
NotificationsClientDep = Annotated[
    NotificationsClientImpl, Depends(_get_notifications_client)
]


# --- Диспетчер уведомлений (синглтон уровня приложения) ---
# Все use case'ы используют ОДИН OrderNotifier: фоновые задачи отправки
# уведомлений переживают завершение HTTP-запроса и не теряются при сборке
# мусора; при остановке приложения ожидаем их завершения (shutdown_resources).

_notification_dispatcher_singleton: NotificationDispatcher | None = None


def _get_notification_dispatcher(settings: SettingsDep) -> NotificationDispatcher:
    global _notification_dispatcher_singleton
    if _notification_dispatcher_singleton is None:
        _notification_dispatcher_singleton = NotificationDispatcher(
            _get_notifications_client(settings)
        )
    return _notification_dispatcher_singleton


NotificationDispatcherDep = Annotated[
    NotificationDispatcher, Depends(_get_notification_dispatcher)
]


def get_order_notifier() -> OrderNotifier:
    """Синглтонный нотификатор для фонового консьюмера (вне HTTP-запроса)."""
    return _get_notification_dispatcher(_get_settings()).notifier


# --- Фабрики use case'ов ---


def get_create_order_use_case(
    uow: UowDep,
    catalog_client: CatalogClientDep,
    payments_client: PaymentsClientDep,
    dispatcher: NotificationDispatcherDep,
    settings: SettingsDep,
) -> CreateOrderUseCase:
    """Создает сценарий оформления заказа (оплата + уведомления)."""
    return CreateOrderUseCase(
        uow,
        catalog_client,
        payments_client=payments_client,
        callback_url=settings.payment_callback_url,
        notifier=dispatcher.notifier,
    )


def get_get_order_use_case(
    uow: UowDep,
) -> GetOrderUseCase:
    """Создает сценарий получения заказа."""
    return GetOrderUseCase(uow)


def get_process_payment_callback_use_case(
    uow: UowDep,
    dispatcher: NotificationDispatcherDep,
    settings: SettingsDep,
) -> ProcessPaymentCallbackUseCase:
    """Создает сценарий обработки callback-а от Payments Service."""
    return ProcessPaymentCallbackUseCase(
        uow,
        order_events_topic=settings.order_events_topic,
        notifier=dispatcher.notifier,
    )


CreateOrderUseCaseDep = Annotated[
    CreateOrderUseCase, Depends(get_create_order_use_case)
]
GetOrderUseCaseDep = Annotated[GetOrderUseCase, Depends(get_get_order_use_case)]
ProcessPaymentCallbackUseCaseDep = Annotated[
    ProcessPaymentCallbackUseCase, Depends(get_process_payment_callback_use_case)
]


# --- Kafka-компоненты: publisher, outbox-воркер, consumer ---
# Синглтоны уровня приложения: создаются и запускаются из lifespan (main.py),
# переживают все запросы; остановка — в shutdown_resources().

_kafka_publisher_singleton: KafkaEventPublisher | None = None
_outbox_worker_singleton: OutboxWorker | None = None
_shipment_consumer_singleton: ShipmentConsumer | None = None


def get_kafka_publisher() -> KafkaEventPublisher:
    """Возвращает синглтон Kafka-публикатора (создается при первом обращении)."""
    global _kafka_publisher_singleton
    if _kafka_publisher_singleton is None:
        _kafka_publisher_singleton = KafkaEventPublisher(_get_settings())
    return _kafka_publisher_singleton


async def handle_shipment_event(event: ShipmentReceivedEvent) -> None:
    """Обработчик входящих событий доставки для Kafka-консьюмера.

    Создает новый UoW на каждое событие (фоновый консьюмер живёт вне
    HTTP-запроса) и запускает use case с Inbox-идемпотентностью.
    """
    settings = _get_settings()
    uow = UnitOfWorkImpl(_get_session_factory(settings))
    use_case = HandleShipmentEventUseCase(
        uow,
        notifier=get_order_notifier(),
    )
    await use_case(event)


def get_outbox_worker() -> OutboxWorker:
    """Синглтон outbox-воркера: переносит события из таблицы Outbox в Kafka."""
    global _outbox_worker_singleton
    if _outbox_worker_singleton is None:
        settings = _get_settings()
        _outbox_worker_singleton = OutboxWorker(
            uow_factory=lambda: UnitOfWorkImpl(_get_session_factory(settings)),
            publisher=get_kafka_publisher(),
            settings=settings,
        )
    return _outbox_worker_singleton


def get_shipment_consumer() -> ShipmentConsumer:
    """Синглтон консьюмера событий Shipping Service."""
    global _shipment_consumer_singleton
    if _shipment_consumer_singleton is None:
        _shipment_consumer_singleton = ShipmentConsumer(
            settings=_get_settings(),
            handler=handle_shipment_event,
        )
    return _shipment_consumer_singleton


async def start_background_components() -> None:
    """Запуск Kafka-компонентов из lifespan приложения.

    Публикатор подключается к брокеру; воркер и консьюмер стартуют как
    фоновые задачи. Сбой подключения к Kafka логируется, но не валит
    HTTP-сервис: outbox накапливает события, консьюмер переподключается.
    """
    try:
        await get_kafka_publisher().start()
    except Exception:
        logger.exception("Failed to start Kafka publisher; will retry via workers")
    await get_outbox_worker().start()
    await get_shipment_consumer().start()


async def shutdown_resources() -> None:
    """Закрывает ресурсы инфраструктуры при остановке приложения."""
    global _catalog_client_singleton, _payments_client_singleton
    global _notifications_client_singleton
    global _kafka_publisher_singleton, _outbox_worker_singleton
    global _shipment_consumer_singleton
    global _notification_dispatcher_singleton
    # Сначала дожидаемся фоновых отправок уведомлений — они используют
    # HTTP-клиент Notifications Service, который закрывается ниже.
    if _notification_dispatcher_singleton is not None:
        await _notification_dispatcher_singleton.wait_pending()
        _notification_dispatcher_singleton = None
    if _shipment_consumer_singleton is not None:
        await _shipment_consumer_singleton.stop()
        _shipment_consumer_singleton = None
    if _outbox_worker_singleton is not None:
        await _outbox_worker_singleton.stop()
        _outbox_worker_singleton = None
    if _kafka_publisher_singleton is not None:
        await _kafka_publisher_singleton.stop()
        _kafka_publisher_singleton = None
    if _catalog_client_singleton is not None:
        await _catalog_client_singleton.close()
        _catalog_client_singleton = None
    if _payments_client_singleton is not None:
        await _payments_client_singleton.close()
        _payments_client_singleton = None
    if _notifications_client_singleton is not None:
        await _notifications_client_singleton.close()
        _notifications_client_singleton = None
