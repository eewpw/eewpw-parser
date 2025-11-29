import json
import os
from importlib import resources
from pathlib import Path
from typing import Optional


CONFIG_ROOT_OVERRIDE: Optional[Path] = None


def set_config_root_override(path: Optional[Path]) -> None:
    global CONFIG_ROOT_OVERRIDE
    CONFIG_ROOT_OVERRIDE = path


def get_package_config_path(rel_path: str) -> Path:
    return resources.files("eewpw_parser.configs") / rel_path


def _repo_config_root() -> Optional[Path]:
    repo_root = Path(__file__).resolve().parents[2] / "configs"
    return repo_root if repo_root.is_dir() else None


def _env_config_root() -> Optional[Path]:
    env_root = os.environ.get("EEWPW_PARSER_CONFIG_ROOT")
    if not env_root:
        return None
    root_path = Path(env_root)
    return root_path if root_path.is_dir() else None


def get_config_path(rel_path: str) -> Path:
    if CONFIG_ROOT_OVERRIDE:
        candidate = CONFIG_ROOT_OVERRIDE / rel_path
        if candidate.exists():
            return candidate

    env_root = _env_config_root()
    if env_root:
        candidate = env_root / rel_path
        if candidate.exists():
            return candidate

    repo_root = _repo_config_root()
    if repo_root:
        candidate = repo_root / rel_path
        if candidate.exists():
            return candidate

    pkg_candidate = get_package_config_path(rel_path)
    if pkg_candidate.exists():
        return Path(pkg_candidate)

    raise FileNotFoundError(f"Config not found for relative path: {rel_path}")


def open_config_json(rel_path: str) -> dict:
    path = get_config_path(rel_path)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
