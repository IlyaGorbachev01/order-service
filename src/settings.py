import os
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


def _build_database_url_from_parts() -> str | None:
    """Собирает DATABASE_URL из отдельных переменных окружения.

    LMS-портал предоставляет PostgreSQL через переменные POSTGRES_*
    (POSTGRES_CONNECTION_STRING вида postgres://user:password@host:port,
    POSTGRES_DATABASE_NAME, POSTGRES_HOST, POSTGRES_PORT).
    Схема postgresql:// не поддерживается asyncpg напрямую,
    поэтому приводим её к postgresql+asyncpg:// и добавляем имя БД.
    """
    conn_str = os.environ.get("POSTGRES_CONNECTION_STRING")
    if not conn_str:
        return None

    url = conn_str.strip()
    body = url.split("://", 1)[1] if "://" in url else url

    credentials, _, host_part = body.rpartition("@")
    host = os.environ.get("POSTGRES_HOST")
    port = os.environ.get("POSTGRES_PORT")
    db_name = os.environ.get("POSTGRES_DATABASE_NAME", "")

    if host:
        netloc = f"{host}:{port}" if port else host
    else:
        # хост/порт берём из connection string, имя БД в пути отбрасываем
        netloc = host_part.split("/", 1)[0]

    path = f"/{db_name}" if db_name else ""
    if credentials:
        return f"postgresql+asyncpg://{credentials}@{netloc}{path}"
    return f"postgresql+asyncpg://{netloc}{path}"


def _default_database_url() -> str:
    return _build_database_url_from_parts() or (
        "postgresql+asyncpg://postgres:postgres@localhost:5432/order_db"
    )


class Settings(BaseSettings):
    """Настройки приложения, загружаемые из переменных окружения."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Портал передаёт много служебных переменных — не падаем на них
    )

    # База данных: приоритет у явного DATABASE_URL, иначе сборка из POSTGRES_*
    database_url: str = _default_database_url()

    # Capashino API (Catalog / Payments)
    capashino_internal_url: str = (
        "http://student-system-capashino-web.student-system-capashino.svc:8000"
    )
    capashino_api_token: str = "your-api-token-here"

    # Notifications Service
    notifications_base_url: str = (
        "http://student-system-capashino-web.student-system-capashino.svc:8000"
    )
    # Токен для Notifications Service; если не задан — используется
    # capashino_api_token (единый ключ для всех сервисов Capashino).
    notifications_api_token: str = ""

    # HTTP-таймаут для вызовов внешних сервисов, сек.
    http_timeout: float = 10.0

    # Формат k8s internal DNS: <service-name>.<namespace>.svc:<порт>.
    order_service_internal_url: str = ""

    # Путь endpoint'а callback-а внутри этого сервиса
    payment_callback_path: str = "/api/orders/payment-callback"

    # Приложение
    debug: bool = False

    # Kafka: интеграция с Shipping Service через события.
    kafka_bootstrap_servers: str = "kafka.kafka.svc.cluster.local:9092"
    # Consumer group Order Service для событий от Shipping Service.
    kafka_consumer_group_id: str = ""
    # Топик исходящих событий Order Service (order.paid)
    order_events_topic: str = "student_system-order.events"
    # Топик входящих событий Shipping Service (order.shipped / order.cancelled)
    shipment_events_topic: str = "student_system-shipment.events"
    # Интервал опроса outbox-таблицы публикатором, сек.
    outbox_poll_interval: float = 1.0

    def model_post_init(self, __context: object) -> None:
        """Дописать уникальную suffix-у к consumer group по умолчанию.

        В Kubernetes HOSTNAME равен имени пода и уникален для каждой
        реплики — так каждая копия worker'а попадает в собственную группу
        и получает все события топика (см. комментарий поля). Локально
        (без HOSTNAME) добавляется PID процесса. Явно заданная
        KAFKA_CONSUMER_GROUP_ID остаётся без изменений.
        """
        if not self.kafka_consumer_group_id:
            pod_identity = os.environ.get("HOSTNAME") or f"pid-{os.getpid()}"
            self.kafka_consumer_group_id = f"order-service-{pod_identity}"

    @property
    def payment_callback_url(self) -> str:
        """Полный внутренний URL для приёма callback-ов от Payments Service."""
        if not self.order_service_internal_url:
            return ""
        base = self.order_service_internal_url.rstrip("/")
        return f"{base}{self.payment_callback_path}"

    @property
    def notifications_api_key(self) -> str:
        """Токен для Notifications Service.

        Если NOTIFICATIONS_API_TOKEN не задан, используется общий токен
        Capashino (X-API-Key одинаков для всех сервисов портала).
        """
        return self.notifications_api_token or self.capashino_api_token


@lru_cache
def get_settings() -> Settings:
    """Возвращает кэшированный экземпляр настроек."""
    return Settings()
