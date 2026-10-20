from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки приложения, загружаемые из переменных окружения."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",  # Строгий контроль: запрещаем неизвестные переменные окружения
    )

    # База данных
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/order_db"

    # Capashino API
    capashino_internal_url: str = (
        "http://student-system-capashino-web.student-system-capashino.svc:8000"
    )
    capashino_api_token: str = "your-api-token-here"

    # Приложение и инфраструктура (для будущего Шага 3)
    debug: bool = False
    kafka_bootstrap_servers: str = "kafka.kafka.svc.cluster.local:9092"


@lru_cache
def get_settings() -> Settings:
    """Возвращает кэшированный экземпляр настроек."""
    return Settings()
