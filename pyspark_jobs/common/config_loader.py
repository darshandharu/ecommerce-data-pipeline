"""Load YAML configs with ``${ENV_VAR}`` interpolation.

Keeping config loading in one place means every job resolves paths,
BigQuery datasets and Spark settings the same way and fails fast with a
clear :class:`ConfigError` when something is missing.
"""
from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from pyspark_jobs.common.exceptions import ConfigError

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _interpolate(value: Any) -> Any:
    """Recursively replace ``${VAR}`` tokens with environment values."""
    if isinstance(value, str):
        def repl(match: re.Match) -> str:
            var = match.group(1)
            env = os.getenv(var)
            if env is None:
                # leave unresolved tokens visible so misconfig is obvious
                return match.group(0)
            return env
        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


@lru_cache(maxsize=None)
def load_config(path: str | os.PathLike) -> dict:
    """Load and env-interpolate a YAML config file (cached)."""
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Config file not found: {p}")
    try:
        with p.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - defensive
        raise ConfigError(f"Could not parse YAML {p}: {exc}") from exc
    return _interpolate(raw)


def get_source(config: dict, name: str) -> dict:
    """Return the source-table definition for ``name`` from pipeline config."""
    for src in config.get("sources", []):
        if src["name"] == name:
            return src
    raise ConfigError(f"Source '{name}' not defined in pipeline config")
