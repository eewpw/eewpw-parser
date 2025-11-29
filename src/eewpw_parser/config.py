# -*- coding: utf-8 -*-
import json
import os
from typing import Dict, Any, Optional
from functools import lru_cache
from pathlib import Path

from eewpw_parser.config_loader import open_config_json
ALGO_CONFIG_MAP = {
    "finder": "finder.json",
    "vs": "vs.json",
}


def config_filename_for_algo(algo: str) -> str:
    try:
        return ALGO_CONFIG_MAP[algo]
    except KeyError:
        supported = ", ".join(sorted(ALGO_CONFIG_MAP))
        raise ValueError(f"Unsupported algo '{algo}'. Supported: {supported}")


def load_config(algo_cfg_path: str = "finder.json") -> Dict[str, Any]:
    """
    Load and merge global and algorithm-specific configuration files.
    configs/global.json is loaded first, then algo_cfg_path is loaded and merged on top.
    These configurations are used to guide the parsing process in the dialect parsers.
    """

    def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(a)
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = v
        return out

    g = open_config_json("global.json")
    a = open_config_json(algo_cfg_path)
    return _deep_merge(g, a)


@lru_cache(maxsize=None)
def load_profile(relative_path: str) -> dict:
    """
    Load a profile JSON from the configured configs root.

    Example:
        load_profile("profiles/finder_time_vs_mag.json")
        load_profile("finder_time_vs_mag.json")  # profile paths are resolved under profiles/
    """
    rel_path = relative_path
    if not rel_path.startswith("profiles/"):
        rel_path = f"profiles/{rel_path}"

    try:
        return open_config_json(rel_path)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


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
