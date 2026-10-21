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

    # Capashino API
    capashino_internal_url: str = (
        "http://student-system-capashino-web.student-system-capashino.svc:8000"
    )
    capashino_api_token: str = "your-api-token-here"

    # Формат k8s internal DNS: <service-name>.<namespace>.svc:<порт>.
    order_service_internal_url: str = ""

    # Путь endpoint'а callback-а внутри этого сервиса
    payment_callback_path: str = "/api/orders/payment-callback"

    # Приложение и инфраструктура (для будущего Шага 3)
    debug: bool = False
    kafka_bootstrap_servers: str = "kafka.kafka.svc.cluster.local:9092"

    @property
    def payment_callback_url(self) -> str:
        """Полный внутренний URL для приёма callback-ов от Payments Service."""
        if not self.order_service_internal_url:
            return ""
        return (
            f"{self.order_service_internal_url.rstrip('/')}{self.payment_callback_path}"
        )


@lru_cache
def get_settings() -> Settings:
    """Возвращает кэшированный экземпляр настроек."""
    return Settings()
