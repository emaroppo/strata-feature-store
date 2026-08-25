"""The spec subpackage does no I/O, and imports nothing that does.

This is the test that keeps a later package split a directory move rather than
a refactor, and it is a test rather than a convention because the boundary
erodes invisibly. In a development checkout everything is installed, so an
``import sqlalchemy`` added to a spec module for one small convenience works
perfectly and nothing complains until somebody tries to depend on spec alone.

Checked two ways. The static pass reads the source, so a forbidden import is
named with the file it appears in. The dynamic pass actually imports every spec
module in a clean interpreter and inspects what got loaded, which also catches
an import arriving transitively through a third party.
"""

from __future__ import annotations

import ast
import pkgutil
import subprocess
import sys
from pathlib import Path

import pytest

import feature_store.spec

#: Anything that opens a socket, a file handle it did not create, or a database.
FORBIDDEN = ("sqlalchemy", "alembic", "psycopg", "boto3", "botocore", "duckdb", "pyarrow", "polars")

SPEC_DIR = Path(feature_store.spec.__file__).parent


def spec_modules() -> list[str]:
    return [
        f"feature_store.spec.{m.name}"
        for m in pkgutil.iter_modules([str(SPEC_DIR)])
        if not m.ispkg
    ]


def test_there_are_spec_modules_to_check():
    """Guards against the suite passing because it found nothing."""
    assert spec_modules(), "no spec modules discovered; the purity check would be vacuous"


@pytest.mark.parametrize("path", sorted(SPEC_DIR.glob("*.py")), ids=lambda p: p.name)
def test_no_forbidden_import_in_source(path: Path):
    tree = ast.parse(path.read_text(), filename=str(path))
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            found.append(node.module)
    offending = [
        name
        for name in found
        if name.split(".")[0] in FORBIDDEN or name.startswith("feature_store.store")
    ]
    assert not offending, f"{path.name} imports {offending}, which the spec boundary forbids"


def test_no_forbidden_import_at_runtime():
    """Import every spec module in a clean interpreter and see what came with it."""
    modules = spec_modules()
    program = (
        "import sys\n"
        f"for name in {modules!r}:\n"
        "    __import__(name)\n"
        f"leaked = sorted(n for n in sys.modules if n.split('.')[0] in {FORBIDDEN!r})\n"
        "leaked += sorted(n for n in sys.modules if n.startswith('feature_store.store'))\n"
        "print(','.join(leaked))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", program], capture_output=True, text=True, check=True
    )
    leaked = [n for n in result.stdout.strip().split(",") if n]
    assert not leaked, f"importing feature_store.spec pulled in {leaked}"
