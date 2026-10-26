#!/bin/bash
set -e

# Запускаем миграции
uv run alembic upgrade head

# Фоновые компоненты (Outbox-relay + Kafka-consumer) — отдельный процесс из bin/
# (требование ТЗ, п.20). В кластере LMS разворачивается только один Deployment
# с этим контейнером, поэтому воркер поднимается здесь как дочерний процесс и
# переживает перезапуски uvicorn-воркеров. Для многосервисного деплоя можно
# отключить флаг RUN_WORKER=false и запускать run-worker.sh вторым контейнером.
RUN_WORKER="${RUN_WORKER:-true}"

WORKER_PID=""
API_PID=""

start_worker() {
    if [ "$RUN_WORKER" = "true" ]; then
        uv run python -m src.bin.worker &
        WORKER_PID=$!
    fi
}

start_api() {
    uv run uvicorn src.main:app --host 0.0.0.0 --port 8000 &
    API_PID=$!
}

shutdown_all() {
    [ -n "$API_PID" ] && kill -TERM "$API_PID" 2>/dev/null || true
    [ -n "$WORKER_PID" ] && kill -TERM "$WORKER_PID" 2>/dev/null || true
}

trap shutdown_all TERM INT EXIT

start_worker
start_api

while true; do
    # Собираем живые PID'ы (мертвые заведомо не передаём в wait -n,
    # иначе он возвращает ошибку и цикл крутится всхолостую).
    LIVE_PIDS=()
    [ -n "$API_PID" ] && kill -0 "$API_PID" 2>/dev/null && LIVE_PIDS+=("$API_PID")
    [ -n "$WORKER_PID" ] && kill -0 "$WORKER_PID" 2>/dev/null && LIVE_PIDS+=("$WORKER_PID")

    if [ "${#LIVE_PIDS[@]}" -gt 0 ]; then
        # Ждём завершения ЛЮБОГО из живых процессов (bash >= 4.3).
        wait -n "${LIVE_PIDS[@]}" 2>/dev/null || true
    else
        sleep 1
    fi

    # Если дочерний воркер умер — перезапускаем его.
    if [ "$RUN_WORKER" = "true" ]; then
        if [ -z "$WORKER_PID" ] || ! kill -0 "$WORKER_PID" 2>/dev/null; then
            start_worker
        fi
    fi

    # Если завершился API (аварийно или штатно) — поднимаем заново.
    # Важно: раньше при падении воркера скрипт выходил вместе с PID 1 и
    # подача становилась недоступной (502 Bad Gateway на время рестарта
    # подом). Теперь контейнер всегда держит оба процесса.
    if [ -z "$API_PID" ] || ! kill -0 "$API_PID" 2>/dev/null; then
        sleep 1
        start_api
    fi
done
