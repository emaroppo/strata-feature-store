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
    from strata.feature_store.store import config as store_config

    return store_config.load(config_path)


def _engine(settings):
    # The way every strata database is opened. On Postgres it is
    # create_engine and nothing more; the one place to change if a
    # connection setting ever matters.
    from strata.common import database

    return database.engine(settings.catalog.with_password())


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
    from strata.feature_store.store import catalog
    from strata.feature_store.store.runs import run as open_run

    settings = _settings(config)
    with open_run(_engine(settings), "ingest_release", f"namespace {name}", {"name": name}) as run:
        catalog.ensure_namespace(run, name, description=description, external_ref=external_ref)
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
    from strata.feature_store.store import catalog, ingest

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
    from strata.feature_store.store import catalog

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
    from strata.feature_store.store import catalog, ingest

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
    from strata.feature_store.store import catalog

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
    from strata.feature_store.store import catalog, ingest

    settings = _settings(config)
    try:
        written = ingest.fetch_release(
            _engine(settings), settings, digest_prefix=digest, destination=out
        )
    except catalog.CatalogError as exc:
        _fail(str(exc))
    console.print(f"[green]wrote[/green] {written}")


@app.command("crosswalk")
def crosswalk_register(
    name: str = typer.Argument(..., help="Name specs will pin, e.g. vdem_to_iso3."),
    release: str = typer.Argument(..., help="Release digest of the crosswalk file."),
    config: Path | None = typer.Option(None),
) -> None:
    """Freeze a crosswalk release as a pinned version.

    The one-to-one check runs here, so a version that exists is safe to use.
    """
    from strata.feature_store.store import crosswalk as cw

    settings = _settings(config)
    try:
        result = cw.register(_engine(settings), settings, name=name, digest_prefix=release)
    except cw.CrosswalkError as exc:
        _fail(str(exc))
    console.print(
        f"[green]crosswalk[/green] {name} v{result.version_num}  "
        f"{result.rows:,} rows, {result.distinct_sources:,} distinct source ids\n"
        f"  key {result.object_key}",
        soft_wrap=True,
    )


@app.command("standardise")
def standardise_cmd(
    # --spec rather than --config: every other command uses --config for
    # config.toml, and one flag meaning two things is how the wrong file gets
    # passed on a tired evening.
    config_file: Path = typer.Option(..., "--spec", exists=True, help="Standardisation config."),
    release: str = typer.Argument(..., help="Release digest, or an unambiguous prefix."),
    set_: list[str] = typer.Option(
        [], "--set", help="Override a config field, as dotted.path=value. Repeatable."
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Count and validate, write nothing."),
    memory_limit: str = typer.Option("8GB", help="DuckDB memory limit before it spills."),
    work_dir: Path | None = typer.Option(None, help="Where to stage and spill. Needs room."),
    config: Path | None = typer.Option(None, help="Path to config.toml."),
) -> None:
    """Turn one release into long-format facts."""
    import json

    from strata.feature_store.store import standardise as std

    overrides: dict[str, object] = {}
    for item in set_:
        if "=" not in item:
            _fail(f"--set expects dotted.path=value, got {item!r}")
        key, _, raw = item.partition("=")
        try:
            overrides[key] = json.loads(raw)
        except json.JSONDecodeError:
            overrides[key] = raw

    settings = _settings(config)
    try:
        spec = std.load_spec(config_file, overrides)
    except Exception as exc:
        _fail(f"{config_file}: {exc}")

    # A melt of a few hundred million facts runs for minutes with nothing to
    # show for it. Progress goes to stderr so redirecting stdout still gets a
    # clean result line.
    from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

    bar = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total} batches"),
        TextColumn("[dim]{task.fields[facts]}[/dim]"),
        TimeElapsedColumn(),
        console=err,
        transient=True,
    )
    task = None

    def on_progress(done: int, total: int, facts: int) -> None:
        nonlocal task
        if task is None:
            task = bar.add_task(spec.feature_group, total=total, facts="")
        bar.update(task, completed=done, facts=f"{facts:,} facts")

    try:
        with bar:
            result = std.standardise(
                _engine(settings),
                settings,
                spec,
                digest_prefix=release,
                work_dir=work_dir,
                memory_limit=memory_limit,
                dry_run=dry_run,
                progress=on_progress,
            )
    except std.StandardiseError as exc:
        _fail(str(exc))

    if dry_run:
        console.print(
            f"[yellow]dry run[/yellow]  {result.rows_written:,} facts from "
            f"{result.features_registered:,} columns. Nothing written."
        )
        return
    verb = "reused" if result.reused_existing else "wrote"
    console.print(
        f"[green]{verb}[/green] {result.rows_written:,} facts, "
        f"{result.features_registered:,} features registered\n"
        f"  group     {spec.feature_group} (id {result.feature_group_id})\n"
        f"  partition {result.partition_id}\n"
        f"  key       {result.object_key}\n"
        f"  run       {result.run_id}",
        soft_wrap=True,
    )


def _compose_table(rows) -> Table:
    """Long-format composition rows, with timestamps trimmed to dates.

    A cutoff and a knowledge time both print as full timestamps otherwise,
    which wraps every row across two lines and makes a panel unreadable.
    """
    table = Table(box=None, pad_edge=False)
    for column in ("entity", "spine", "data_through", "feature", "value", "knowable", "age"):
        table.add_column(column, overflow="fold")

    def day(value) -> str:
        return "" if value is None else str(getattr(value, "date", lambda: value)())

    for row in rows:
        value = row[4] if row[4] is not None else row[5]
        table.add_row(
            str(row[0]),
            day(row[1]),
            day(row[2]),
            str(row[3]),
            "[dim]stale[/dim]" if value is None else f"{value}",
            day(row[6]),
            "" if row[7] is None else str(row[7]),
        )
    return table


@app.command("compose")
def compose_cmd(
    spec_file: Path = typer.Option(..., "--spec", exists=True, help="Composition spec."),
    freeze: str = typer.Option(None, help="query|materialized. Overrides the spec."),
    preview: int = typer.Option(0, help="Resolve and print N rows, freezing nothing."),
    memory_limit: str = typer.Option("4GB"),
    config: Path | None = typer.Option(None),
) -> None:
    """Resolve a composition, and pin it so it answers the same way forever."""
    import json

    from strata.feature_store.spec.compose import ComposeSpec
    from strata.feature_store.store import compose as cmp

    settings = _settings(config)
    payload = json.loads(spec_file.read_text())
    if freeze:
        payload["freeze"] = {"mode": freeze}
    try:
        spec = ComposeSpec.model_validate(payload)
    except Exception as exc:
        _fail(f"{spec_file}: {exc}")

    engine = _engine(settings)
    if preview:
        try:
            with engine.connect() as connection:
                plan = cmp.plan(connection, spec, settings)
            rows = cmp.resolve(plan, memory_limit=memory_limit).limit(preview).fetchall()
        except cmp.ComposeError as exc:
            _fail(str(exc))
        console.print(_compose_table(rows))
        err.print(f"[dim]watermark {plan.watermark} — nothing frozen[/dim]")
        return

    try:
        frozen = cmp.freeze(engine, settings, spec, memory_limit=memory_limit)
    except cmp.ComposeError as exc:
        _fail(str(exc))
    console.print(
        f"[green]froze[/green] {spec.name}@{frozen.version_num} "
        f"({frozen.resolution_mode})\n"
        f"  rows      {frozen.rows:,}\n"
        f"  spec      {frozen.content_hash[:16]}\n"
        f"  watermark {frozen.watermark}"
        + (f"\n  key       {frozen.object_prefix}" if frozen.object_prefix else ""),
        soft_wrap=True,
    )


@app.command("read")
def read_cmd(
    target: str = typer.Argument(..., help="dataset, or dataset@version."),
    latest: bool = typer.Option(
        False, "--latest", help="Ignore the pinned watermark and use everything known now."
    ),
    limit: int = typer.Option(20, help="Rows to print. 0 for all."),
    out: Path | None = typer.Option(None, help="Write parquet here instead of printing."),
    memory_limit: str = typer.Option("4GB"),
    config: Path | None = typer.Option(None),
) -> None:
    """Resolve a frozen version, warning about anything its pin withholds."""
    from strata.feature_store.store import compose as cmp

    settings = _settings(config)
    name, _, version = target.partition("@")
    try:
        relation, warning = cmp.read(
            _engine(settings),
            settings,
            name,
            int(version) if version else None,
            latest=latest,
            memory_limit=memory_limit,
        )
    except cmp.ComposeError as exc:
        _fail(str(exc))

    if warning.get("facts"):
        # Pinning withholds data, so it must not do so silently.
        err.print(
            f"[yellow]note:[/yellow] {warning['facts']:,} fact(s) in "
            f"{', '.join(warning['groups'])} were knowable by {warning['cutoff']} but "
            f"arrived after this version's watermark, so they are excluded. Newest such "
            f"knowledge_time: {warning['newest_knowledge_time']}. "
            f"Freeze a new version to include them, or pass --latest to see both."
        )

    if out:
        relation.to_parquet(str(out))
        console.print(f"[green]wrote[/green] {out}")
        return

    rows = relation.fetchall() if limit == 0 else relation.limit(limit).fetchall()
    console.print(_compose_table(rows))


@app.command("versions")
def versions_cmd(
    name: str = typer.Argument(...),
    config: Path | None = typer.Option(None),
) -> None:
    """Every frozen version of a dataset."""
    import sqlalchemy as sa

    from strata.feature_store.store.tables import dataset, dataset_version

    settings = _settings(config)
    with _engine(settings).connect() as connection:
        rows = (
            connection.execute(
                sa.select(dataset_version)
                .join(dataset, dataset.c.id == dataset_version.c.dataset_id)
                .where(dataset.c.name == name)
                .order_by(dataset_version.c.version_num)
            )
            .mappings()
            .all()
        )
    if not rows:
        _fail(f"no dataset named {name!r}")
    table = Table(box=None, pad_edge=False)
    for column in ("version", "mode", "spec hash", "watermark", "frozen"):
        table.add_column(column)
    for row in rows:
        table.add_row(
            str(row["version_num"]),
            row["resolution_mode"],
            row["content_hash"][:16],
            str(row["ingest_watermark"]),
            f"{row['frozen_at']:%Y-%m-%d %H:%M}",
        )
    console.print(table)


@app.command("verify")
def verify_cmd(
    deep: bool = typer.Option(False, "--deep", help="Re-hash bytes and reconcile the registry."),
    orphans: bool = typer.Option(False, "--orphans", help="Find objects no row points at."),
    prune_orphans: bool = typer.Option(
        False, "--prune-orphans", help="Delete them. Refuses unless the rest of the store agrees."
    ),
    run: int | None = typer.Option(None, "--run", help="Scope to one run's outputs."),
    yes: bool = typer.Option(False, "--yes", help="Skip the confirmation for --prune-orphans."),
    config: Path | None = typer.Option(None),
) -> None:
    """Check that the catalog and the store agree."""
    from strata.feature_store.store import verify as verifier

    settings = _settings(config)

    if prune_orphans:
        try:
            preview = verifier.prune_orphans(_engine(settings), settings, dry_run=True)
        except verifier.UndoError as exc:
            _fail(str(exc))
        count = len(preview["would_remove"])
        if not count:
            console.print("[green]no orphans to remove[/green]")
            return
        if not yes:
            err.print(
                f"[yellow]{count} object(s) would be deleted.[/yellow] They are the ones "
                "no catalog row points at — debris from runs that died between writing "
                "bytes and recording them. Pass --yes to proceed."
            )
            for key in preview["would_remove"][:5]:
                err.print(f"    {key}", soft_wrap=True)
            if count > 5:
                err.print(f"    [dim]... and {count - 5} more[/dim]")
            raise typer.Exit(1)
        result = verifier.prune_orphans(_engine(settings), settings)
        console.print(f"[green]removed[/green] {len(result['removed'])} orphaned object(s)")
        return

    found = verifier.verify(
        _engine(settings), settings, deep=deep, orphans=orphans, run_id=run
    )

    console.print(f"checked [bold]{found.checked}[/bold] catalogued object(s)")
    for label, items, colour in (
        ("missing", found.missing, "red"),
        ("wrong size", found.wrong_size, "red"),
        ("wrong digest", found.wrong_digest, "red"),
        ("registry drift", found.registry_drift, "yellow"),
        ("orphaned", found.orphans, "yellow"),
        ("from unfinished runs", found.unfinished, "dim"),
    ):
        if items:
            console.print(f"[{colour}]{len(items)} {label}[/{colour}]")
            for item in items[:10]:
                console.print(f"    {item}", soft_wrap=True)
            if len(items) > 10:
                console.print(f"    [dim]... and {len(items) - 10} more[/dim]")
    if found.ok:
        console.print("[green]catalog and store agree[/green]")
    else:
        raise typer.Exit(1)


@runs_app.command("undo")
def runs_undo(
    run_id: int = typer.Argument(...),
    config: Path | None = typer.Option(None),
) -> None:
    """Roll a run's outputs back. Refuses if anything depends on them."""
    from strata.feature_store.store import verify as verifier

    settings = _settings(config)
    try:
        result = verifier.undo(_engine(settings), settings, run_id)
    except verifier.UndoError as exc:
        _fail(str(exc))
    console.print(
        f"[green]rolled back[/green] run {run_id}: "
        f"{result['partitions']} partition(s), {len(result['objects_removed'])} object(s) removed"
    )


@release_app.command("prune")
def release_prune(
    digest: str = typer.Argument(..., help="Digest, or an unambiguous prefix."),
    yes: bool = typer.Option(False, "--yes", help="Skip the confirmation."),
    config: Path | None = typer.Option(None),
) -> None:
    """Drop a release's bytes, keeping its row as a tombstone.

    Never automatic. Re-standardising from the original file is the whole reason
    releases are kept, and this is the one command that gives that up.
    """
    from strata.feature_store.store import verify as verifier

    settings = _settings(config)
    if not yes:
        err.print(
            "[yellow]This deletes the original bytes.[/yellow] Re-standardising this "
            "release becomes impossible without downloading it again, and publishers "
            "do not always still serve the same vintage. Pass --yes to proceed."
        )
        raise typer.Exit(1)
    try:
        result = verifier.prune_release(_engine(settings), settings, digest)
    except verifier.UndoError as exc:
        _fail(str(exc))
    console.print(f"[green]pruned[/green] {result['sha256'][:12]}", soft_wrap=True)


@app.command("entity-list")
def entity_list_cmd(
    name: str = typer.Argument(..., help="What specs will pin, e.g. region_r."),
    ids: list[str] = typer.Option([], "--id", help="An entity id. Repeatable."),
    from_file: Path | None = typer.Option(None, help="One id per line."),
    namespace: str = typer.Option(..., help="The vocabulary these ids belong to."),
    description: str | None = typer.Option(None),
    config: Path | None = typer.Option(None),
) -> None:
    """Freeze a named set of entity ids as a new version."""
    from strata.feature_store.store import catalog
    from strata.feature_store.store.runs import run as open_run

    settings = _settings(config)
    members = list(ids)
    if from_file:
        members += [line.strip() for line in from_file.read_text().splitlines() if line.strip()]
    if not members:
        _fail("give at least one --id, or --from-file")

    try:
        with open_run(
            _engine(settings), "ingest_release", f"entity list {name}", {"name": name}
        ) as run:
            _, version_num = catalog.add_entity_list(
                run,
                name=name,
                entity_namespace=namespace,
                members=members,
                description=description,
            )
    except catalog.CatalogError as exc:
        _fail(str(exc))
    console.print(
        f"[green]entity list[/green] {name} v{version_num} — {len(set(members))} id(s)"
    )


@runs_app.command("ls")
def runs_ls(
    limit: int = typer.Option(20),
    kind: str | None = typer.Option(None),
    config: Path | None = typer.Option(None),
) -> None:
    """Recent runs."""
    import sqlalchemy as sa

    from strata.feature_store.store.tables import run as run_table

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

    from strata.feature_store.store.tables import release as release_table

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

    from strata.feature_store.store.tables import run as run_table
    from strata.feature_store.store.tables import run_input, run_output

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

    from strata.feature_store.store.tables import run as run_table
    from strata.feature_store.store.tables import run_input, run_output

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
