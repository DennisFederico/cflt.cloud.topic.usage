#!/usr/bin/env python3
"""Render Prometheus config from template and environment variables."""

from __future__ import annotations

import os
from pathlib import Path
from string import Template

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = ROOT / ".cflt-local" / "prometheus" / "prometheus.yml.tmpl"
OUTPUT_PATH = ROOT / ".cflt-local" / "prometheus" / "prometheus.yml"

REQUIRED_VARS = (
    "CFLT_CLOUD_API_KEY",
    "CFLT_CLOUD_API_SECRET",
    "CFLT_CLUSTER_ID",
)

OPTIONAL_DEFAULTS = {
    "PROM_SCRAPE_INTERVAL": "1m",
    "PROM_SCRAPE_TIMEOUT": "55s",
}


def _load_dotenv(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def _load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _ensure_required_env() -> None:
    missing = [name for name in REQUIRED_VARS if not os.getenv(name, "").strip()]
    if missing:
        joined = ", ".join(missing)
        raise SystemExit(f"Missing required environment variables: {joined}")


def main() -> None:
    _load_dotenv(ROOT / ".env")
    _ensure_required_env()

    template_text = _load_text(TEMPLATE_PATH)
    env_values = dict(os.environ)
    for key, value in OPTIONAL_DEFAULTS.items():
        env_values.setdefault(key, value)

    rendered = Template(template_text).substitute(env_values)
    OUTPUT_PATH.write_text(rendered, encoding="utf-8")

    print(f"Rendered {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
