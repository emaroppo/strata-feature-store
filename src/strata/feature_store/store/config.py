"""Where things are, and how to reach them.

``config.toml`` holds locations and is gitignored; the environment holds
credentials and nothing else; ``configs/*.json`` hold meaning and are tracked.
See ``strata/docs/adr/0019``.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

DEFAULT_CONFIG_NAME = "config.toml"

#: Read for credentials, and nowhere else.
ACCESS_KEY_VAR = "FEATURE_STORE_S3_ACCESS_KEY"
SECRET_KEY_VAR = "FEATURE_STORE_S3_SECRET_KEY"
PASSWORD_VAR = "PGPASSWORD"


class ConfigError(RuntimeError):
    """Raised with what is missing and where to put it."""


@dataclass(frozen=True, slots=True)
class CatalogSettings:
    url: str

    def with_password(self) -> str:
        """The URL with ``$PGPASSWORD`` spliced in when it is set and the URL carries none."""
        password = os.environ.get(PASSWORD_VAR)
        if not password or "@" not in self.url:
            return self.url
        scheme, _, rest = self.url.partition("://")
        userinfo, _, hostpart = rest.partition("@")
        if ":" in userinfo:
            return self.url
        return f"{scheme}://{userinfo}:{password}@{hostpart}"


@dataclass(frozen=True, slots=True)
class StorageSettings:
    endpoint_url: str
    bucket: str
    region: str = "garage"
    addressing_style: str = "path"

    @property
    def access_key(self) -> str:
        return _required_env(ACCESS_KEY_VAR)

    @property
    def secret_key(self) -> str:
        return _required_env(SECRET_KEY_VAR)

    @property
    def duckdb_endpoint(self) -> str:
        """The endpoint as DuckDB wants it: host and port, no scheme. See ``docs/adr/0010``."""
        _, _, hostport = self.endpoint_url.partition("://")
        return hostport or self.endpoint_url

    @property
    def use_ssl(self) -> bool:
        return self.endpoint_url.startswith("https://")


@dataclass(frozen=True, slots=True)
class Settings:
    catalog: CatalogSettings
    storage: StorageSettings
    #: The vocabulary every stored parquet is written in; see ``docs/adr/0005``.
    canonical_namespace: str
    path: Path | None = None


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(
            f"{name} is not set. Credentials come from the environment, never from "
            f"{DEFAULT_CONFIG_NAME}. Copy .env.example to .env and fill it in, or export it."
        )
    return value


def find_config(start: Path | None = None) -> Path:
    """Search upward from *start* for ``config.toml``."""
    here = (start or Path.cwd()).resolve()
    for directory in (here, *here.parents):
        candidate = directory / DEFAULT_CONFIG_NAME
        if candidate.is_file():
            return candidate
    raise ConfigError(
        f"no {DEFAULT_CONFIG_NAME} found in {here} or any parent. "
        "Copy config.example.toml to config.toml and edit it."
    )


def load(path: Path | None = None) -> Settings:
    """Load settings from *path*, or from the nearest ``config.toml``."""
    resolved = path or find_config()
    try:
        raw = tomllib.loads(resolved.read_text())
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{resolved} is not valid TOML: {exc}") from exc

    missing = [section for section in ("catalog", "storage") if section not in raw]
    if missing:
        raise ConfigError(f"{resolved} is missing section(s): {', '.join(missing)}")

    try:
        catalog = CatalogSettings(url=raw["catalog"]["url"])
        storage_raw = dict(raw["storage"])
        storage = StorageSettings(
            endpoint_url=storage_raw.pop("endpoint_url"),
            bucket=storage_raw.pop("bucket"),
            region=storage_raw.pop("region", "garage"),
            addressing_style=storage_raw.pop("addressing_style", "path"),
        )
    except KeyError as exc:
        raise ConfigError(f"{resolved} is missing required key {exc}") from exc
    if storage_raw:
        raise ConfigError(
            f"{resolved} has unknown key(s) in [storage]: {', '.join(sorted(storage_raw))}"
        )

    namespace = raw.get("entities", {}).get("canonical_namespace")
    if not namespace:
        raise ConfigError(
            f"{resolved} must set entities.canonical_namespace. Every stored parquet is "
            "written in one vocabulary, and leaving it implicit means nothing can check it."
        )

    return Settings(
        catalog=catalog,
        storage=storage,
        canonical_namespace=namespace,
        path=resolved,
    )
