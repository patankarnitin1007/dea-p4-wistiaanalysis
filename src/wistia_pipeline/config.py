"""Loads non-secret ingestion defaults from config/pipeline_config.yaml.

Glue job parameters passed on the command line always take precedence;
these values only fill in defaults for local development so the job can
be run without a full argument list.
"""
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "pipeline_config.yaml"


def load_yaml_defaults(path=None):
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not config_path.exists():
        return {}
    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}
    return raw.get("ingestion", {})
