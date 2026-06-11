"""Unit tests for config loading + env interpolation."""
from __future__ import annotations

import textwrap

from pyspark_jobs.common import config_loader as cfg
from pyspark_jobs.common.exceptions import ConfigError


def test_env_interpolation(tmp_path, monkeypatch):
    monkeypatch.setenv("MY_BUCKET", "gs://demo")
    p = tmp_path / "c.yaml"
    p.write_text(textwrap.dedent("""
        paths:
          bronze: ${MY_BUCKET}/bronze
    """))
    cfg.load_config.cache_clear()
    out = cfg.load_config(str(p))
    assert out["paths"]["bronze"] == "gs://demo/bronze"


def test_missing_file_raises():
    cfg.load_config.cache_clear()
    try:
        cfg.load_config("does/not/exist.yaml")
        assert False
    except ConfigError:
        pass


def test_get_source(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("sources:\n  - name: orders\n    file: o.csv\n")
    cfg.load_config.cache_clear()
    conf = cfg.load_config(str(p))
    assert cfg.get_source(conf, "orders")["file"] == "o.csv"
    try:
        cfg.get_source(conf, "missing")
        assert False
    except ConfigError:
        pass
