import json
import os
from importlib import resources
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


CONFIG_ROOT_OVERRIDE: Optional[Path] = None


def set_config_root_override(path: Optional[Path]) -> None:
    global CONFIG_ROOT_OVERRIDE
    CONFIG_ROOT_OVERRIDE = path


def get_package_config_path(rel_path: str) -> Path:
    return resources.files("eewpw_parser.configs") / rel_path


def get_env_config_root_raw() -> Optional[Path]:
    env_root = os.environ.get("EEWPW_PARSER_CONFIG_ROOT")
    if not env_root:
        return None
    return Path(env_root)


def _env_config_root() -> Optional[Path]:
    root_path = get_env_config_root_raw()
    return root_path if root_path and root_path.is_dir() else None


def get_config_path_resolution(
    rel_path: str,
    include_unchecked: bool = True,
) -> Tuple[Optional[Path], List[Dict[str, Any]]]:
    trace: List[Dict[str, Any]] = []
    winner: Optional[Path] = None

    if CONFIG_ROOT_OVERRIDE:
        cli_candidate = CONFIG_ROOT_OVERRIDE / rel_path
        cli_exists = cli_candidate.exists()
        trace.append(
            {
                "source": "--config-root",
                "root": CONFIG_ROOT_OVERRIDE,
                "candidate": cli_candidate,
                "checked": True,
                "winner": cli_exists,
                "exists": cli_exists,
                "status": "winner" if cli_exists else "missing",
            }
        )
        if cli_exists:
            winner = cli_candidate
            if not include_unchecked:
                return winner, trace
    else:
        trace.append(
            {
                "source": "--config-root",
                "root": None,
                "candidate": None,
                "checked": False,
                "winner": False,
                "exists": None,
                "status": "not_set",
            }
        )

    env_root_raw = get_env_config_root_raw()
    env_root = _env_config_root()
    if winner is not None:
        if not include_unchecked:
            return winner, trace
        env_status = "not_set"
        if env_root_raw and env_root is None:
            env_status = "invalid_root"
        elif env_root is not None:
            env_status = "not_checked_after_winner"
        trace.append(
            {
                "source": "EEWPW_PARSER_CONFIG_ROOT",
                "root": env_root_raw,
                "candidate": (env_root / rel_path) if env_root else None,
                "checked": False,
                "winner": False,
                "exists": None,
                "status": env_status,
            }
        )
    elif env_root is not None:
        env_candidate = env_root / rel_path
        env_exists = env_candidate.exists()
        trace.append(
            {
                "source": "EEWPW_PARSER_CONFIG_ROOT",
                "root": env_root,
                "candidate": env_candidate,
                "checked": True,
                "winner": env_exists,
                "exists": env_exists,
                "status": "winner" if env_exists else "missing",
            }
        )
        if env_exists:
            winner = env_candidate
            if not include_unchecked:
                return winner, trace
    elif include_unchecked:
        trace.append(
            {
                "source": "EEWPW_PARSER_CONFIG_ROOT",
                "root": env_root_raw,
                "candidate": None,
                "checked": False,
                "winner": False,
                "exists": None,
                "status": "invalid_root" if env_root_raw else "not_set",
            }
        )

    pkg_root = get_package_config_path("")
    pkg_candidate = get_package_config_path(rel_path)
    if winner is not None:
        if not include_unchecked:
            return winner, trace
        trace.append(
            {
                "source": "packaged defaults",
                "root": Path(pkg_root),
                "candidate": Path(pkg_candidate),
                "checked": False,
                "winner": False,
                "exists": None,
                "status": "not_checked_after_winner",
            }
        )
    else:
        pkg_exists = pkg_candidate.exists()
        trace.append(
            {
                "source": "packaged defaults",
                "root": Path(pkg_root),
                "candidate": Path(pkg_candidate),
                "checked": True,
                "winner": pkg_exists,
                "exists": pkg_exists,
                "status": "winner" if pkg_exists else "missing",
            }
        )
        if pkg_exists:
            winner = Path(pkg_candidate)

    return winner, trace


def get_config_path(rel_path: str) -> Path:
    winner, _ = get_config_path_resolution(rel_path, include_unchecked=False)
    if winner is not None:
        return winner
    raise FileNotFoundError(f"Config not found for relative path: {rel_path}")


def open_config_json(rel_path: str) -> dict:
    path = get_config_path(rel_path)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
