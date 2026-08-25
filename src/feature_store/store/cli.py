"""The command line, one sub-app per noun.

Commands are thin: they parse arguments, build a config, and call into the
module that does the work. Nothing here decides anything, so that the same
operations remain callable as a library.
"""

from __future__ import annotations

import typer

app = typer.Typer(
    name="feature-store",
    help="Point-in-time correct storage, standardisation and retrieval for revised panel data.",
    no_args_is_help=True,
    add_completion=False,
)


@app.command()
def version() -> None:
    """Print the installed version."""
    from importlib.metadata import version as _version

    typer.echo(_version("feature-store"))


if __name__ == "__main__":  # pragma: no cover
    app()
