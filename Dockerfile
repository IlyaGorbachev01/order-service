# Официальный легкий образ с предустановленным Python и uv
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

# Копируем файлы зависимостей
COPY pyproject.toml uv.lock ./

# Синхронизируем зависимости (без dev-пакетов для продакшена)
RUN uv sync --frozen --no-dev

# Копируем исходный код, миграции и скрипт запуска
COPY src/ ./src/
COPY alembic/ ./alembic/
COPY alembic.ini ./
COPY run.sh ./

# Делаем скрипт запуска исполняемым
RUN chmod +x ./run.sh

EXPOSE 8000

CMD ["bash", "./run.sh"]
