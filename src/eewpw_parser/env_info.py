# -*- coding: utf-8 -*-
"""
Module for building a brief report of the environment and configuration 
resolution for the EEWPW Parser.
"""
import platform
import sys
from pathlib import Path
from typing import List

import eewpw_parser
from eewpw_parser import config_loader


def _yes_no(value: bool) -> str:
    """ Return 'yes' for True and 'no' for False. """
    return "yes" if value else "no"


def _append_path_details(lines: List[str], path: Path) -> None:
    abs_path = path.expanduser().resolve(strict=False)
    lines.append(f"     path      : {abs_path}")
    lines.append(f"     exists    : {_yes_no(path.exists())}")
    lines.append(f"     directory : {_yes_no(path.is_dir())}")


def _packaged_profile_paths() -> List[str]:
    profiles_dir = config_loader.get_package_config_path("profiles")
    if not profiles_dir.exists() or not profiles_dir.is_dir():
        return []

    rel_paths: List[str] = []
    for path in sorted(profiles_dir.iterdir(), key=lambda p: p.name):
        if path.is_file() and path.name.endswith(".json"):
            rel_paths.append(f"profiles/{path.name}")
    return rel_paths


def _winner_source(trace: List[dict]) -> str:
    for step in trace:
        if step.get("status") == "winner":
            return str(step.get("source"))
    return "none"


def _is_compact_case(trace: List[dict]) -> bool:
    if not trace:
        return False
    # Compact form is only allowed when the first lookup source wins.
    return trace[0].get("status") == "winner"


def _format_detailed_step(step: dict) -> str:
    source = step["source"]
    status = step["status"]
    candidate = step.get("candidate")
    root = step.get("root")

    if status == "winner":
        return f"[x] {source}"
    if status == "not_set":
        return f"[ ] {source} (not set)"
    if status == "invalid_root":
        return f"[ ] {source} {root} (invalid: not a directory)"
    if status == "not_checked_after_winner":
        return f"[ ] {source}"
    if status == "missing":
        return f"[ ] {source} {candidate} (missing)"
    return f"[ ] {source} ({status})"


def build_env_report() -> str:
    lines: List[str] = []

    lines.append("EEWPW Parser Environment")
    lines.append("=" * 32)
    lines.append("")

    lines.append("Python")
    lines.append(f"  Executable : {sys.executable}")
    lines.append(f"  Version    : {platform.python_version()}")
    lines.append(f"  Working dir: {Path.cwd()}")
    lines.append("")

    lines.append("Package")
    package_path = Path(eewpw_parser.__file__).resolve().parent
    lines.append(f"  Module path: {package_path}")
    lines.append("")

    cli_root = config_loader.CONFIG_ROOT_OVERRIDE
    env_root_raw = config_loader.get_env_config_root_raw()
    pkg_root = Path(config_loader.get_package_config_path(""))

    # Check the folders in look-up order. If not set, indicate so...
    # Otherwise, show the path.
    lines.append("Config lookup order")
    lines.append("  1. --config-root")
    if cli_root is None:
        lines.append("     (not set)")
    else:
        _append_path_details(lines, cli_root)

    lines.append("  2. EEWPW_PARSER_CONFIG_ROOT")
    if env_root_raw is None:
        lines.append("     (not set)")
    else:
        _append_path_details(lines, env_root_raw)

    lines.append("  3. packaged defaults")
    _append_path_details(lines, pkg_root)
    lines.append("")

    runtime_files = ["global.json", "annotations.json", *_packaged_profile_paths()]

    lines.append("Resolved files")
    lines.append("-" * 28)
    for rel_path in runtime_files:
        _, trace = config_loader.get_config_path_resolution(rel_path)
        lines.append(f"{rel_path}")

        if _is_compact_case(trace):
            # Mark the winning source with [x] and skip details for compact cases.
            lines.append(f"[x] {_winner_source(trace)}")
        else:
            for step in trace:
                lines.append(_format_detailed_step(step))
            if not any(step.get("status") == "winner" for step in trace):
                lines.append("[ ] no source selected (not found)")
        lines.append("")

    profiles = [p for p in runtime_files if p.startswith("profiles/")]
    lines.append("Profiles summary")
    lines.append(f"  profiles considered ({len(profiles)}):")
    for rel_path in profiles:
        lines.append(f"  - {rel_path}")

    return "\n".join(lines)
