"""frozen versions are immutable

Revision ID: e50d4c2786c1
Revises: dac07246db87
Created: 2026-08-26 10:21:01.272319
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e50d4c2786c1"
down_revision: str | None = "dac07246db87"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Invariant 5, enforced by the database rather than by discipline.

    A frozen version is the identity of a result. Editing one changes what an
    old pin resolves to without changing its hash, which is the quietest way a
    reproducible answer stops being reproducible - and the tempting fix while
    correcting a mistake is exactly a one-row UPDATE. Corrections make new
    versions; that is what version_num is for.

    Rows may still be deleted by `run undo`, but only for a run that froze
    nothing and that nothing depends on, which undo checks before it gets here.
    """
    op.execute(
        """
        CREATE OR REPLACE FUNCTION feature_store_frozen_version_is_immutable()
        RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION
                'dataset_version % is frozen and cannot be modified. '
                'Corrections make a new version; editing this one would change '
                'what an existing pin resolves to without changing its hash.',
                OLD.id;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER dataset_version_immutable
        BEFORE UPDATE ON dataset_version
        FOR EACH ROW EXECUTE FUNCTION feature_store_frozen_version_is_immutable();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS dataset_version_immutable ON dataset_version")
    op.execute("DROP FUNCTION IF EXISTS feature_store_frozen_version_is_immutable()")
