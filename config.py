# =============================================================================
# config.py — Unified Configuration Module (Phase 2.3)
#
# Central loader for all pipeline configuration. Computes a SHA256 config
# version hash so every pipeline output can be traced to the exact config
# that produced it. Read by all pipeline stages at startup.
# =============================================================================

import hashlib
import json
import os
from pathlib import Path
from datetime import datetime, timezone

BASE = Path(__file__).resolve().parent
OPENCLAW_HOME = Path(os.path.expanduser("~/.openclaw"))

# ── Config sources (order matters for hash stability) ────────────────────────

_CONFIG_FILES = [
    OPENCLAW_HOME / "signal_weights.json",
    OPENCLAW_HOME / ".env",
    BASE / "country_config.py",
    BASE / "vertical_cpc_reference.json",
]

_RUNTIME_OVERRIDES = {
    "DATAFORSEO_DAILY_BUDGET": os.environ.get("DATAFORSEO_DAILY_BUDGET", "500"),
    "CACHE_TTL_HOURS": "168",
    "VETTING_CAP": "2000",
    "TRANSFORM_CAP": "500",
    "SEMAPHORE_LIMIT": "10",
    "OLLAMA_MODEL": os.environ.get("OLLAMA_MODEL", "qwen3:14b"),
}


def _file_hash(path: Path) -> str:
    """SHA256 of file contents, or empty string if file missing."""
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compute_config_version() -> str:
    """Deterministic SHA256 hash of all config sources + runtime overrides.
    Changes when any config file is modified or env var differs."""
    h = hashlib.sha256()
    for f in sorted(_CONFIG_FILES, key=str):
        h.update(f"{f}:{_file_hash(f)}".encode())
    for k in sorted(_RUNTIME_OVERRIDES):
        h.update(f"{k}={_RUNTIME_OVERRIDES[k]}".encode())
    return h.hexdigest()[:16]


def load_signal_weights() -> dict:
    """Load signal_weights.json written by reflection.py."""
    path = OPENCLAW_HOME / "signal_weights.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"min_cpc_override": {}, "golden_rate_floor": 0.001}


def load_vertical_cpc_reference() -> dict:
    """Load vertical CPC reference data."""
    path = BASE / "vertical_cpc_reference.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


class PipelineConfig:
    """Singleton-style config object available to all pipeline stages."""

    def __init__(self):
        self.version = compute_config_version()
        self.loaded_at = datetime.now(timezone.utc).isoformat()
        self.signal_weights = load_signal_weights()
        self.vertical_cpc = load_vertical_cpc_reference()
        self.base_path = BASE
        self.openclaw_home = OPENCLAW_HOME

        # Runtime params
        self.dataforseo_daily_budget = int(_RUNTIME_OVERRIDES["DATAFORSEO_DAILY_BUDGET"])
        self.cache_ttl_hours = int(_RUNTIME_OVERRIDES["CACHE_TTL_HOURS"])
        self.vetting_cap = int(_RUNTIME_OVERRIDES["VETTING_CAP"])
        self.transform_cap = int(_RUNTIME_OVERRIDES["TRANSFORM_CAP"])
        self.semaphore_limit = int(_RUNTIME_OVERRIDES["SEMAPHORE_LIMIT"])
        self.ollama_model = _RUNTIME_OVERRIDES["OLLAMA_MODEL"]

    def to_dict(self) -> dict:
        return {
            "config_version": self.version,
            "loaded_at": self.loaded_at,
            "dataforseo_daily_budget": self.dataforseo_daily_budget,
            "cache_ttl_hours": self.cache_ttl_hours,
            "vetting_cap": self.vetting_cap,
            "transform_cap": self.transform_cap,
            "semaphore_limit": self.semaphore_limit,
            "ollama_model": self.ollama_model,
        }

    def stamp(self, record: dict) -> dict:
        """Embed config_version into a pipeline output record."""
        record["config_version"] = self.version
        return record


# Module-level singleton — import and use directly
pipeline_config = PipelineConfig()


if __name__ == "__main__":
    print(f"Config version: {pipeline_config.version}")
    print(json.dumps(pipeline_config.to_dict(), indent=2))
