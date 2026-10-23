"""widen_external_id_columns

Revision ID: a6df68fccfdf
Revises: 21c4fee805d4
Create Date: 2026-10-23 14:27:29.665420

"""
from typing import Sequence, Union

from alembic import context, op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a6df68fccfdf'
down_revision: Union[str, Sequence[str], None] = '21c4fee805d4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _is_postgres() -> bool:
    return context.get_context().dialect.name.startswith("postgresql")


def _alter(table: str, column: str, length: int, *, nullable: bool = False) -> None:
    op.alter_column(
        table,
        column,
        existing_type=sa.String(length=None),
        type_=sa.String(length=length),
        existing_nullable=nullable,
    )


def upgrade() -> None:
    """Upgrade schema."""
    if not _is_postgres():
        # SQLite: длина varchar не enforced, менять нечего.
        return
    # orders
    _alter("orders", "id", 128)
    _alter("orders", "user_id", 128)
    _alter("orders", "item_id", 128)
    _alter("orders", "idempotency_key", 255)
    _alter("orders", "payment_id", 128, nullable=True)
    # payment_callbacks
    _alter("payment_callbacks", "payment_id", 128)
    _alter("payment_callbacks", "order_id", 128)
    # outbox_events
    _alter("outbox_events", "aggregate_id", 128)
    # inbox_events
    _alter("inbox_events", "event_id", 255)
    _alter("inbox_events", "order_id", 128)


def downgrade() -> None:
    """Downgrade schema."""
    if not _is_postgres():
        return
    _alter("inbox_events", "order_id", 36)
    _alter("inbox_events", "event_id", 128)
    _alter("outbox_events", "aggregate_id", 36)
    _alter("payment_callbacks", "order_id", 36)
    _alter("payment_callbacks", "payment_id", 36)
    _alter("orders", "payment_id", 36, nullable=True)
    _alter("orders", "idempotency_key", 64)
    _alter("orders", "item_id", 36)
    _alter("orders", "user_id", 36)
    _alter("orders", "id", 36)
