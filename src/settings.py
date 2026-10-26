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


def _default_order_service_internal_url() -> str:
    """Собирает внутренний URL сервиса из переменных окружения Kubernetes.

    Payments Service доставляет результат платежа на callback_url, который
    Order Service передаёт при создании платежа. Если callback_url пустой
    или внешний, платёж доходит до ``succeeded`` в Payments, но заказ
    навсегда остаётся в ``NEW`` (callback не приходит) — автотест падает
    на ожидании ``SHIPPED``/уведомлений.

    Поэтому адрес вычисляем автоматически, даже если ORDER_SERVICE_INTERNAL_URL
    не задана в конфигурации деплоя. Kubernetes внедряет в каждый под
    стандартные переменные сервисов: для собственного сервиса пода это
    ``SERVICE_HOST`` и ``SERVICE_PORT`` (например, для сервиса
    ``student-ilyagorbachev01-order-service`` — переменные
    ``STUDENT_ILYAGORBACHEV01_ORDER_SERVICE_SERVICE_HOST/_PORT``; имена
    приводятся к верхнему регистру с заменой «-» на «_»). Дополнительно
    пробуем разобрать имя пода (StatefulSet/Deployment:
    ``<pod>.svc.cluster.local`` не всегда доступен, поэтому используем
    namespace из downward API). Порядок поиска:

    1. явные SERVICE_HOST/SERVICE_PORT (если их внедрил кластер);
    2. первая пара ``*_SERVICE_HOST``/``*_SERVICE_PORT``, где хост совпадает
       с именем сервиса из переменной портала (ORDER_SERVICE_NAME) или где
       суффикс перекрывается с ним;
    3. ничего не найдено — пустая строка (в логах будет предупреждение).

    Формат результата: canonical internal DNS ``http://<service>.<ns>.svc:<port>``.
    """
    port = os.environ.get("SERVICE_PORT") or ""
    host = os.environ.get("SERVICE_HOST") or ""

    if not (host and port):
        # Собираем пары <PREFIX>_SERVICE_HOST / <PREFIX>_SERVICE_PORT из env.
        pairs: dict[str, dict[str, str]] = {}
        for name, value in os.environ.items():
            if name.endswith("_SERVICE_HOST"):
                pairs.setdefault(name[: -len("_SERVICE_HOST")], {})["host"] = value
            elif name.endswith("_SERVICE_PORT"):
                pairs.setdefault(name[: -len("_SERVICE_PORT")], {})["port"] = value

        # Кандидат: собственный сервис order-service — ищем по ключевым словам.
        service_hints = [
            os.environ.get("ORDER_SERVICE_NAME", ""),
            os.environ.get("SERVICE_NAME", ""),
            os.environ.get("APP_NAME", ""),
            "order-service",
        ]
        hints = [h.strip("-").replace("-", "_").upper() for h in service_hints if h]

        chosen_prefix: str | None = None
        chosen: dict[str, str] | None = None
        for prefix, pair in pairs.items():
            if pair.get("host") and pair.get("port"):
                if any(hint and hint in prefix for hint in hints):
                    chosen_prefix, chosen = prefix, pair
                    break
        if chosen is None:  # fallback: первая полная пара (обычно их немного)
            for prefix, pair in pairs.items():
                if pair.get("host") and pair.get("port"):
                    chosen_prefix, chosen = prefix, pair
                    break
        if chosen is not None:
            host = chosen["host"]
            port = chosen["port"]

    if not (host and port):
        return ""

    namespace = os.environ.get("POD_NAMESPACE") or os.environ.get("NAMESPACE", "")

    if chosen_prefix:
        # Kubernetes не позволяет указать disableServiceLinkVariables, а
        # SERVICE_HOST — это ClusterIP. Надёжнее собрать canonical internal
        # DNS из имени сервиса: префикс переменной — имя сервиса с «-» -> «_».
        # Отбрасываем завершающий _SERVICE (K8S добавляет его к префиксу).
        svc_name = chosen_prefix
        if svc_name.endswith("_SERVICE"):
            svc_name = svc_name[: -len("_SERVICE")]
        svc_name = svc_name.lower().replace("_", "-")
        if namespace:
            return f"http://{svc_name}.{namespace}.svc:{port}"
        return f"http://{svc_name}:{port}"

    # Canonical internal DNS: <name>.<namespace>.svc:<port>. Явный SERVICE_HOST
    # может быть IP или именем; дополняем namespace, если это короткое имя.
    if namespace and "." not in host and ":" not in host:
        host = f"{host}.{namespace}.svc"
    return f"http://{host}:{port}"


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

    # Внутренний адрес этого сервиса для приёма callback-ов от Payments
    # Service. Формат k8s internal DNS: <service-name>.<namespace>.svc:<порт>.
    # Если не задан явно — вычисляется автоматически из переменных окружения,
    # которые внедряет Kubernetes (см. _default_order_service_internal_url):
    # пустой callback_url = платёж никогда не подтверждается на стороне заказа,
    # заказ залипает в NEW и автотест не доходит до SHIPPED.
    order_service_internal_url: str = _default_order_service_internal_url()

    # Порт HTTP-сервиса (используется при автоопределении внутреннего URL)
    service_port: int = 8000

    # Путь endpoint'а callback-а внутри этого сервиса
    payment_callback_path: str = "/api/orders/payment-callback"

    # Приложение
    debug: bool = False

    # Kafka: интеграция с Shipping Service через события.
    kafka_bootstrap_servers: str = "kafka.kafka.svc.cluster.local:9092"
    # Consumer group Order Service для событий от Shipping Service.
    # По умолчанию вычисляется из имени пода (HOSTNAME в Kubernetes — он
    # уникален для каждого реплики/процесса): каждая копия консьюмера
    # подписывается на топик `student_system-shipment.events` отдельно и
    # получает ВСЕ сообщения (effective broadcasting). Дедупликация
    # повторных доставок обеспечивается Inbox-паттерном по event_id, а
    # переходы статуса идемпотентны на уровне доменной машины состояний.
    # Так исключается классическая потеря событий при delivery-guarantee
    # настройках кластера (offset коммитится после первой доставки группе,
    # остальные копии их не получают) и зависание смещения из-за ошибок
    # ретрах-политики одной копии. Явное значение KAFKA_CONSUMER_GROUP_ID
    # всегда имеет приоритет (например, для общей группы в тестах).
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

        # Если ORDER_SERVICE_INTERNAL_URL не задана явно и автоопределение
        # из service-link переменных не сработало (например, enableServiceLinks
        # выключен или портал не создал Service к моменту старта пода),
        # пробуем собрать canonical internal DNS из well-known переменных
        # Kubernetes: POD_NAME + NAMESPACE/SERVICE_ACCOUNT. Имя сервиса в
        # кластере портала совпадает с Deployment'ом вида
        # student-<user>-order-service; если точно восстановить имя не удалось,
        # оставляем пустое значение — в логах будет явное предупреждение от
        # CreateOrderUseCase (callback_url is empty).
        if not self.order_service_internal_url:
            pod_name = os.environ.get("POD_NAME", "")
            namespace = os.environ.get("NAMESPACE") or os.environ.get(
                "POD_NAMESPACE", ""
            )
            # Разворачиваем имя пода Deployment'а назад к имени сервиса:
            # student-x-order-service-<replicaset-hash>-<pod-suffix> ->
            # student-x-order-service
            if pod_name and namespace:
                parts = pod_name.split("-")
                for cut in (2, 3):  # replicaset+suffix / suffix-only схемы
                    if len(parts) > cut:
                        candidate = "-".join(parts[:-cut])
                        if candidate.endswith("-order-service"):
                            self.order_service_internal_url = (
                                f"http://{candidate}.{namespace}.svc:"
                                f"{self.service_port}"
                            )
                            break

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
