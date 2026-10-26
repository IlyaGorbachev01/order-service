#!/bin/bash
set -e

# Запускаем миграции
uv run alembic upgrade head

# Фоновые компоненты (Outbox-relay + Kafka-consumer)
RUN_WORKER="${RUN_WORKER:-true}"

WORKER_PID=""

start_worker() {
    if [ "$RUN_WORKER" = "true" ]; then
        uv run python -m src.bin.worker &
        WORKER_PID=$!
    fi
}

stop_worker() {
    if [ -n "$WORKER_PID" ] && kill -0 "$WORKER_PID" 2>/dev/null; then
        kill -TERM "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
    fi
}

trap stop_worker TERM INT EXIT

start_worker

# Запускаем FastAPI сервер в цикле: если uvicorn аварийно завершится,
# воркер (если жив) останавливаем и поднимаем всё заново.
while true; do
    # Если дочерний воркер умер — перезапускаем его.
    if [ "$RUN_WORKER" = "true" ]; then
        if [ -z "$WORKER_PID" ] || ! kill -0 "$WORKER_PID" 2>/dev/null; then
            start_worker
        fi
    fi

    uv run uvicorn src.main:app --host 0.0.0.0 --port 8000
    API_EXIT=$?

    if [ "$API_EXIT" = "0" ]; then
        break
    fi
    sleep 1
done