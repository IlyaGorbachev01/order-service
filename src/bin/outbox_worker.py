"""Точка запуска outbox-воркера отдельным процессом.

Модуль оставлен как совместимый алиас: полный состав фоновых компонентов
(outbox relay + консьюмер событий Shipping Service) собирается в
`src.bin.worker`, и именно он используется в деплое (`run-worker.sh`).

Запуск (тот же образ, что и у API; переменные окружения — те же):

    python -m src.bin.outbox_worker   # эквивалентно python -m src.bin.worker

Воркер НЕ запускается из lifespan API-процесса (требование ТЗ — отдельный
процесс в bin/). Несколько копий воркера безопасны: партии захватываются
транзакцией `FOR UPDATE SKIP LOCKED`, поэтому одно событие не публикуется
двумя экземплярами параллельно. Graceful shutdown по SIGTERM/SIGINT.
"""

from src.bin.worker import WorkerRuntime, main, run_forever

__all__ = ["WorkerRuntime", "main", "run_forever"]


if __name__ == "__main__":
    main()
