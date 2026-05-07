# -*- coding: utf-8 -*-
import json
import os
from typing import Dict, Any, Optional
from functools import lru_cache
from pathlib import Path
from eewpw_parser.config_loader import open_config_json


_FINDER_DIALECT_ALIASES = {
    "scfinder": "scfinder",
    "native_finder": "native_finder",
    "native-finder": "native_finder",
    "nativefinder": "native_finder",
    "finder": "native_finder",
    "native_finder_legacy": "native_finder_legacy",
    "native-finder-legacy": "native_finder_legacy",
    "nativefinderlegacy": "native_finder_legacy",
    "finder_legacy": "native_finder_legacy",
    "finder-legacy": "native_finder_legacy",
    "finderlegacy": "native_finder_legacy",
    "shakealert": "shakealert",
}

_LEGACY_PROFILE_ANNOTATION_CONTEXT = {
    "profiles/finder_time_vs_mag.json": ("time_vs_magnitude", "finder", "native_finder"),
    "profiles/scfinder_time_vs_mag.json": ("time_vs_magnitude", "finder", "scfinder"),
    "profiles/vs_time_vs_mag.json": ("time_vs_magnitude", "vs", "scvsmag"),
    "profiles/plum_time_vs_mag.json": ("time_vs_magnitude", "plum", "plum"),
    "profiles/epic_time_vs_mag.json": ("time_vs_magnitude", "epic", "shakealert"),
    "profiles/gfast_time_vs_mag.json": ("time_vs_magnitude", "gfast", "shakealert"),
    "profiles/eqinfo_time_vs_mag.json": ("time_vs_magnitude", "eqinfo", "shakealert"),
}


def load_global_config() -> Dict[str, Any]:
    """
    Load the global configuration (no algorithm-specific layer).
    """
    return open_config_json("global.json")


def _normalize_annotation_lookup_key(algo: str, dialect: Optional[str]) -> Optional[str]:
    algo_norm = (algo or "").strip().lower()
    dialect_norm = (dialect or "").strip().lower()

    if algo_norm == "finder":
        canonical = _FINDER_DIALECT_ALIASES.get(dialect_norm)
        if canonical is None:
            return None
        return f"finder/{canonical}"

    if not algo_norm or not dialect_norm:
        return None
    return f"{algo_norm}/{dialect_norm}"


def resolve_annotation_profile(
    target: str,
    algo: str,
    dialect: Optional[str],
) -> Optional[Dict[str, Any]]:
    lookup_key = _normalize_annotation_lookup_key(algo, dialect)
    if lookup_key is None:
        return None

    try:
        cfg = open_config_json("annotations.json")
    except (FileNotFoundError, json.JSONDecodeError):
        return None

    annotations = cfg.get("annotations")
    if not isinstance(annotations, dict):
        return None

    target_cfg = annotations.get(target)
    if not isinstance(target_cfg, dict):
        return None

    patterns = target_cfg.get(lookup_key)
    if not isinstance(patterns, dict):
        return None

    resolved_patterns = dict(patterns)
    resolved_patterns.pop("timestamp_regex", None)
    return {"patterns": resolved_patterns}


@lru_cache(maxsize=None)
def load_profile(
    relative_path: str,
    algo: Optional[str] = None,
    dialect: Optional[str] = None,
    target: Optional[str] = None,
) -> dict:
    """
    Load a profile JSON from the configured configs root.

    Example:
        load_profile("profiles/finder_time_vs_mag.json")
        load_profile("finder_time_vs_mag.json")  # profile paths are resolved under profiles/
    """
    rel_path = relative_path
    if not rel_path.startswith("profiles/"):
        rel_path = f"profiles/{rel_path}"

    legacy_ctx = _LEGACY_PROFILE_ANNOTATION_CONTEXT.get(rel_path)
    resolved_target = target or (legacy_ctx[0] if legacy_ctx else None)
    resolved_algo = algo or (legacy_ctx[1] if legacy_ctx else None)
    resolved_dialect = dialect if dialect is not None else (legacy_ctx[2] if legacy_ctx else None)

    if resolved_target and resolved_algo:
        resolved_annotation_profile = resolve_annotation_profile(
            target=resolved_target,
            algo=resolved_algo,
            dialect=resolved_dialect,
        )
        if resolved_annotation_profile is not None:
            return resolved_annotation_profile

    try:
        profile = open_config_json(rel_path)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

    patterns = profile.get("patterns")
    if isinstance(patterns, dict):
        patterns.pop("timestamp_regex", None)
    return profile


def get_data_root(cfg: Optional[dict]) -> Path:
    """
    Resolve the data root used for live outputs.
    Priority:
    1) EEWPW_DATA_ROOT environment variable
    2) cfg["live"]["data_root"] if present
    3) ./data under current working directory
    """
    env_root = os.environ.get("EEWPW_DATA_ROOT")
    if env_root:
        return Path(env_root)
    live_cfg = (cfg or {}).get("live", {}) if isinstance(cfg, dict) else {}
    data_root = live_cfg.get("data_root")
    if data_root:
        return Path(data_root)
    return Path.cwd() / "data"


def get_live_raw_dir(data_root: Path, algo: str) -> Path:
    """
    Directory where live raw JSONL files for a given algo are stored.
    """
    return data_root / "live" / "raw" / algo


def get_live_daily_jsonl_path(data_root: Path, algo: str, date_str: str) -> Path:
    """
    Daily file path for the given algo and date (YYYY-MM-DD).
    """
    return get_live_raw_dir(data_root, algo) / f"{date_str}_{algo}.jsonl"
