# -*- coding: utf-8 -*-
import json
from typing import Dict, Any
from pathlib import Path
from functools import lru_cache

def load_config(algo_cfg_path: str = "configs/finder.json") -> Dict[str, Any]:
    """
    Load and merge global and algorithm-specific configuration files.
    configs/global.json is loaded first, then algo_cfg_path is loaded and merged on top.
    These configurations are used to guide the parsing process in the dialect parsers.
    """
    base_dir = Path(__file__).resolve().parents[2]
    global_path = base_dir / "configs" / "global.json"
    algo_path = Path(algo_cfg_path)
    if not algo_path.is_absolute():
        algo_path = base_dir / algo_cfg_path

    def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(a)
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = v
        return out


    with global_path.open("r", encoding="utf-8") as f:
        g = json.load(f)
    with algo_path.open("r", encoding="utf-8") as f:
        a = json.load(f)
    return _deep_merge(g, a)


@lru_cache(maxsize=None)
def load_profile(relative_path: str) -> dict:
    """
    Load a profile JSON from the repo-level configs directory.

    Example:
        load_profile("profiles/finder_time_vs_mag.json")
    """
    here = Path(__file__).resolve()
    profile_path = here.parents[2] / "configs" / relative_path

    try:
        with profile_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
