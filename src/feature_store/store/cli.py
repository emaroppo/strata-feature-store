"""The command line, one sub-app per noun.

Commands are thin on purpose: parse arguments, load settings, call the module
that does the work, print what happened. Nothing here decides anything, so every
operation stays callable as a library — which is what a scheduled job or a
notebook will want, and what keeps the CLI from becoming the only way in.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="feature-store",
    help="Point-in-time correct storage, standardisation and retrieval for revised panel data.",
    no_args_is_help=True,
    add_completion=False,
)
source_app = typer.Typer(help="Publishers' products.", no_args_is_help=True)
release_app = typer.Typer(help="Files as downloaded from a source.", no_args_is_help=True)
runs_app = typer.Typer(help="What happened, and what it read and produced.", no_args_is_help=True)
app.add_typer(source_app, name="source")
app.add_typer(release_app, name="release")
app.add_typer(runs_app, name="runs")

console = Console()
err = Console(stderr=True)


def _settings(config_path: Path | None = None):
    from feature_store.store import config as store_config

    return store_config.load(config_path)


def _engine(settings):
    import sqlalchemy as sa

    return sa.create_engine(settings.catalog.with_password())


def _fail(message: str) -> None:
    err.print(f"[red]error:[/red] {message}")
    raise typer.Exit(1)


def _human_bytes(count: int) -> str:
    size = float(count)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:,.0f} {unit}" if unit == "B" else f"{size:,.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"  # pragma: no cover


@app.command()
def version() -> None:
    """Print the installed version."""
    from importlib.metadata import version as _version

    console.print(_version("feature-store"))


@app.command("init-db")
def init_db(config: Path | None = typer.Option(None, help="Path to config.toml.")) -> None:
    """Create or upgrade the catalog schema."""
    import os

    from alembic import command
    from alembic.config import Config

    settings = _settings(config)
    os.environ["FEATURE_STORE_DB_URL"] = settings.catalog.with_password()
    command.upgrade(Config("alembic.ini"), "head")
    console.print("[green]catalog is at head[/green]")


@app.command("namespace")
def namespace_add(
    name: str = typer.Argument(..., help="Vocabulary name, e.g. iso3166-1-alpha3."),
    description: str | None = typer.Option(None),
    external_ref: str | None = typer.Option(None, help="Where the vocabulary is defined."),
    config: Path | None = typer.Option(None),
) -> None:
    """Register an entity vocabulary."""
    from feature_store.store import catalog
    from feature_store.store.runs import run as open_run

    settings = _settings(config)
    with _engine(settings).begin() as connection:
        with open_run(connection, "ingest_release", f"namespace {name}", {"name": name}) as current:
            catalog.ensure_namespace(
                current, name, description=description, external_ref=external_ref
            )
    console.print(f"[green]namespace[/green] {name}")


@source_app.command("register")
def source_register(
    name: str = typer.Argument(..., help="A short stable name for the product."),
    namespace: str = typer.Option(..., help="The vocabulary its entity ids are published in."),
    granularity: str = typer.Option(..., help="annual|quarterly|monthly|weekly|daily|irregular"),
    kind: str = typer.Option("panel", help="panel|crosswalk"),
    licence: str | None = typer.Option(None),
    url: str | None = typer.Option(None, help="Where it came from."),
    description: str | None = typer.Option(None),
    config: Path | None = typer.Option(None),
) -> None:
    """Register a publisher's product. Releases arrive against it."""
    from feature_store.store import catalog, ingest

    settings = _settings(config)
    try:
        source_id = ingest.register_source(
            _engine(settings),
            settings,
            name=name,
            native_namespace=namespace,
            granularity=granularity,
            kind=kind,
            licence=licence,
            source_url=url,
            description=description,
        )
    except catalog.CatalogError as exc:
        _fail(str(exc))
    console.print(f"[green]source[/green] {name} (id {source_id}) in {namespace}, {granularity}")


@source_app.command("ls")
def source_ls(config: Path | None = typer.Option(None)) -> None:
    """List registered sources."""
    from feature_store.store import catalog

    settings = _settings(config)
    with _engine(settings).begin() as connection:
        rows = catalog.list_sources(connection)
        counts = {r["id"]: len(catalog.list_releases(connection, r["id"])) for r in rows}
    table = Table(box=None, pad_edge=False)
    for column in ("name", "kind", "namespace", "granularity", "releases"):
        table.add_column(column)
    for row in rows:
        table.add_row(
            row["name"],
            row["kind"],
            row["native_namespace"],
            row["granularity"],
            str(counts[row["id"]]),
        )
    console.print(table if rows else "[dim]no sources registered[/dim]")


@release_app.command("add")
def release_add(
    path: Path = typer.Argument(..., exists=True, dir_okay=False, help="The file as downloaded."),
    source: str = typer.Option(..., help="Which registered source it came from."),
    released_at: datetime | None = typer.Option(
        None, formats=["%Y-%m-%d"], help="The publisher's vintage date. Not the download date."
    ),
    retrieved_at: datetime | None = typer.Option(
        None, formats=["%Y-%m-%d"], help="When it was fetched. Defaults to now."
    ),
    url: str | None = typer.Option(None),
    config: Path | None = typer.Option(None),
) -> None:
    """Store a downloaded file exactly as it arrived, and record it."""
    from feature_store.store import catalog, ingest

    settings = _settings(config)
    try:
        result = ingest.add_release(
            _engine(settings),
            settings,
            path=path,
            source_name=source,
            released_at=released_at.date() if released_at else None,
            retrieved_at=retrieved_at,
            source_url=url,
        )
    except catalog.CatalogError as exc:
        _fail(str(exc))

    verb = "stored" if result.uploaded else "already present"
    # soft_wrap: an object key broken across lines reads like a corrupted value,
    # and these are meant to be copied.
    console.print(
        f"[green]{verb}[/green] {result.sha256[:12]}  {_human_bytes(result.size_bytes)}\n"
        f"  key      {result.object_key}\n"
        f"  release  {result.release_id}   run {result.run_id}",
        soft_wrap=True,
    )
    if released_at is None:
        err.print(
            "[yellow]note:[/yellow] no --released-at given. Without the publisher's vintage "
            "date this release cannot carry a file_release knowledge_time."
        )


@release_app.command("ls")
def release_ls(
    source: str | None = typer.Option(None, help="Limit to one source."),
    config: Path | None = typer.Option(None),
) -> None:
    """List stored releases."""
    from feature_store.store import catalog

    settings = _settings(config)
    with _engine(settings).begin() as connection:
        source_id = None
        if source:
            found = catalog.get_source(connection, source)
            if found is None:
                _fail(f"no source named {source!r}")
            source_id = found["id"]
        rows = catalog.list_releases(connection, source_id)
        names = {s["id"]: s["name"] for s in catalog.list_sources(connection)}

    table = Table(box=None, pad_edge=False)
    for column in ("digest", "source", "released", "size", "filename"):
        table.add_column(column)
    for row in rows:
        table.add_row(
            row["sha256"][:12],
            names.get(row["source_id"], "?"),
            str(row["released_at"] or "[dim]unknown[/dim]"),
            _human_bytes(row["size_bytes"]),
            row["original_filename"] + (" [dim](pruned)[/dim]" if row["pruned_at"] else ""),
        )
    console.print(table if rows else "[dim]no releases stored[/dim]")


@release_app.command("get")
def release_get(
    digest: str = typer.Argument(..., help="Digest, or enough of a prefix to be unambiguous."),
    out: Path = typer.Option(..., "--out", "-o", help="Where to write it."),
    config: Path | None = typer.Option(None),
) -> None:
    """Pull an original file back out, digest-verified on arrival."""
    from feature_store.store import catalog, ingest

    settings = _settings(config)
    try:
        written = ingest.fetch_release(
            _engine(settings), settings, digest_prefix=digest, destination=out
        )
    except catalog.CatalogError as exc:
        _fail(str(exc))
    console.print(f"[green]wrote[/green] {written}")


@runs_app.command("ls")
def runs_ls(
    limit: int = typer.Option(20),
    kind: str | None = typer.Option(None),
    config: Path | None = typer.Option(None),
) -> None:
    """Recent runs."""
    import sqlalchemy as sa

    from feature_store.store.tables import run as run_table

    settings = _settings(config)
    query = sa.select(run_table).order_by(run_table.c.started_at.desc()).limit(limit)
    if kind:
        query = query.where(run_table.c.kind == kind)
    with _engine(settings).begin() as connection:
        rows = connection.execute(query).mappings().all()

    table = Table(box=None, pad_edge=False)
    for column in ("id", "kind", "status", "started", "name"):
        table.add_column(column)
    colours = {"ok": "green", "failed": "red", "running": "yellow", "rolled_back": "dim"}
    for row in rows:
        colour = colours.get(row["status"], "white")
        table.add_row(
            str(row["id"]),
            row["kind"],
            f"[{colour}]{row['status']}[/{colour}]",
            f"{row['started_at']:%Y-%m-%d %H:%M}",
            row["name"],
        )
    console.print(table if rows else "[dim]no runs[/dim]")


def _describe(ref_type: str, ref_id: int, connection) -> str:
    import sqlalchemy as sa

    from feature_store.store.tables import release as release_table

    if ref_type == "release":
        row = (
            connection.execute(sa.select(release_table).where(release_table.c.id == ref_id))
            .mappings()
            .first()
        )
        if row:
            return f"{row['original_filename']} ({row['sha256'][:12]})"
    return f"#{ref_id}"


@runs_app.command("show")
def runs_show(
    run_id: int = typer.Argument(...),
    lineage: bool = typer.Option(False, "--lineage", help="Walk inputs transitively."),
    config: Path | None = typer.Option(None),
) -> None:
    """One run: what it was, what it read, what it produced."""
    import sqlalchemy as sa

    from feature_store.store.tables import run as run_table
    from feature_store.store.tables import run_input, run_output

    settings = _settings(config)
    with _engine(settings).begin() as connection:
        row = (
            connection.execute(sa.select(run_table).where(run_table.c.id == run_id))
            .mappings()
            .first()
        )
        if row is None:
            _fail(f"no run {run_id}")
        console.print(f"[bold]run {row['id']}[/bold]  {row['kind']}  [dim]{row['status']}[/dim]")
        console.print(f"  {row['name']}")
        console.print(f"  config  {row['config_hash'][:16]}")
        if row["code_git_sha"]:
            dirty = " [yellow](dirty tree)[/yellow]" if row["code_dirty"] else ""
            console.print(f"  code    {row['code_git_sha'][:12]}{dirty}")
        if row["max_knowledge_time"]:
            console.print(f"  knows up to {row['max_knowledge_time']}")
        if row["manifest_key"]:
            console.print(f"  manifest {row['manifest_key']}")

        for label, table_ref in (("reads", run_input), ("writes", run_output)):
            refs = (
                connection.execute(sa.select(table_ref).where(table_ref.c.run_id == run_id))
                .mappings()
                .all()
            )
            for ref in refs:
                console.print(
                    f"  {label:6} {ref['ref_type']:18} "
                    f"{_describe(ref['ref_type'], ref['ref_id'], connection)}"
                )

        if lineage:
            console.print("\n[bold]lineage[/bold]")
            _walk_lineage(connection, run_id, set(), 0)


def _walk_lineage(connection, run_id: int, seen: set[int], depth: int) -> None:
    """Print what produced this run's inputs, and what produced those, and so on."""
    import sqlalchemy as sa

    from feature_store.store.tables import run as run_table
    from feature_store.store.tables import run_input, run_output

    if run_id in seen or depth > 12:
        return
    seen.add(run_id)
    row = (
        connection.execute(sa.select(run_table).where(run_table.c.id == run_id)).mappings().first()
    )
    console.print(f"  {'  ' * depth}run {row['id']}  {row['kind']}  [dim]{row['name']}[/dim]")

    upstream = connection.execute(
        sa.select(run_output.c.run_id)
        .join(
            run_input,
            sa.and_(
                run_input.c.ref_type == run_output.c.ref_type,
                run_input.c.ref_id == run_output.c.ref_id,
            ),
        )
        .where(run_input.c.run_id == run_id)
        .distinct()
    ).scalars()
    for parent in upstream:
        _walk_lineage(connection, parent, seen, depth + 1)


if __name__ == "__main__":  # pragma: no cover
    app()
