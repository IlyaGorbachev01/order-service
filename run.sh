#!/bin/bash
set -e

# Запускаем миграции
uv run alembic upgrade head

# Запускаем FastAPI сервер
uv run uvicorn src.main:app --host 0.0.0.0 --port 8000