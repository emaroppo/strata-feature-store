"""Where things are, and how to reach them.

Three layers, and the split is a rule rather than a habit:

``config.toml``
    Locations. Which database, which endpoint, which bucket, which canonical
    vocabulary. Machine-local and gitignored, because a checkout on another
    host points somewhere else.

the environment
    Credentials, and only credentials. Nothing that is a secret is ever written
    to a file this repository can read, so a config accidentally committed
    leaks a hostname rather than a key.

``configs/*.json``
    Meaning. What a column is, how a feature is named, what an as-of policy is.
    These *are* tracked, because they decide what a number means and belong
    under review with the code that consumes them.

The failure this arrangement prevents is the ordinary one: a credential in a
config file, a config file in a commit, and no way to know how long it was
there.
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
        """The URL with ``$PGPASSWORD`` spliced in, if it is set and absent.

        Kept out of ``config.toml`` so the file stays shareable. A URL that
        already carries a password is left alone, since overriding it silently
        would be worse than either behaviour on its own.
        """
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
        """The endpoint as DuckDB wants it: host and port, no scheme.

        boto3 takes a full URL and DuckDB refuses one. Getting this wrong fails
        with a message about the request rather than about the setting, which is
        why the two forms are derived from one value rather than configured
        twice and allowed to drift apart.
        """
        _, _, hostport = self.endpoint_url.partition("://")
        return hostport or self.endpoint_url

    @property
    def use_ssl(self) -> bool:
        return self.endpoint_url.startswith("https://")


@dataclass(frozen=True, slots=True)
class Settings:
    catalog: CatalogSettings
    storage: StorageSettings
    #: The vocabulary every stored parquet is written in. Configuration rather
    #: than a constant: right for country panels, wrong for actors or firms.
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
