"""crosswalk is a dataset kind

Revision ID: dac07246db87
Revises: b53278f71197
Created: 2026-08-26 00:32:06.181158
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "dac07246db87"
down_revision: str | None = "b53278f71197"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Written by hand. Alembic's autogenerate does not detect a changed CHECK
    # constraint, so it produced an empty migration and the widening silently
    # did not happen -- the failure surfaced later as a CheckViolation on an
    # insert that the model said was fine.
    #
    # This is the standing cost of preferring TEXT + CHECK to native enums. The
    # trade is still right (widening a CHECK is cheap and non-transactional),
    # but every vocabulary change needs its migration written rather than
    # generated.
    # Pass the BARE constraint name. MetaData's naming_convention formats it as
    # ck_%(table_name)s_%(constraint_name)s, so passing "ck_dataset_kind" here
    # asks for "ck_dataset_ck_dataset_kind" and the drop fails on a name that
    # was never created.
    op.drop_constraint("kind", "dataset", type_="check")
    op.create_check_constraint("kind", "dataset", "kind IN ('composed', 'crosswalk')")


def downgrade() -> None:
    op.drop_constraint("kind", "dataset", type_="check")
    op.create_check_constraint("kind", "dataset", "kind IN ('composed')")
