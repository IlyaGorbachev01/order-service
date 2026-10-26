# Отдельный процесс фоновых компонентов Order Service
set -e

uv run python -m src.bin.worker
