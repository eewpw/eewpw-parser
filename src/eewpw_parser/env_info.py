# -*- coding: utf-8 -*-
"""
Module for building a brief report of the environment and configuration 
resolution for the EEWPW Parser.
"""
import inspect
import json
import platform
import re
import sys
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, List, Optional

import eewpw_parser
from eewpw_parser import config as parser_config
from eewpw_parser import config_loader


_ANNOTATION_TARGET = "time_vs_magnitude"


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


def _discover_parser_classes() -> Dict[str, type]:
    root = Path(__file__).resolve().parent / "parsers"
    out: Dict[str, type] = {}
    if not root.exists() or not root.is_dir():
        return out

    for algo_dir in sorted(root.iterdir(), key=lambda p: p.name):
        if not algo_dir.is_dir() or algo_dir.name.startswith("_"):
            continue
        algo = algo_dir.name
        parser_file = algo_dir / f"{algo}_parser.py"
        if not parser_file.exists():
            continue

        module_name = f"eewpw_parser.parsers.{algo}.{algo}_parser"
        try:
            mod = import_module(module_name)
        except Exception:
            continue

        candidates = [
            cls
            for _, cls in inspect.getmembers(mod, inspect.isclass)
            if cls.__module__ == module_name and cls.__name__.endswith("Parser")
        ]
        if not candidates:
            continue

        preferred_name = f"{algo.capitalize()}Parser"
        parser_cls = next((c for c in candidates if c.__name__ == preferred_name), candidates[0])
        out[algo] = parser_cls

    return out


def _discover_default_profile_names(algorithms: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for algo in algorithms:
        module_name = f"eewpw_parser.parsers.{algo}.dialects"
        try:
            mod = import_module(module_name)
        except Exception:
            continue

        profile_names = set()
        for _, cls in inspect.getmembers(mod, inspect.isclass):
            if cls.__module__ != module_name:
                continue
            profile_name = getattr(cls, "PROFILE_NAME", None)
            if isinstance(profile_name, str) and profile_name.startswith("profiles/"):
                profile_names.add(profile_name)

        if len(profile_names) == 1:
            out[algo] = sorted(profile_names)[0]
    return out


def _build_algo_capabilities() -> Dict[str, Dict[str, Any]]:
    parser_classes = _discover_parser_classes()
    default_profiles = _discover_default_profile_names(sorted(parser_classes))
    finder_aliases = getattr(parser_config, "_FINDER_DIALECT_ALIASES", {})

    out: Dict[str, Dict[str, Any]] = {}
    for algo in sorted(parser_classes):
        parser_cls = parser_classes[algo]
        info: Dict[str, Any] = {
            "algo": algo,
            "accepted_literals": [],
            "accepted_input_note": None,
            "canonical": [],
            "default_dialect": None,
            "default_profile": default_profiles.get(algo),
            "alias_map": {},
        }

        default_dialect: Optional[str] = None
        try:
            parser_default = parser_cls({})
            default_dialect = str(getattr(parser_default, "dialect", "")).strip().lower() or None
        except Exception:
            default_dialect = None
        info["default_dialect"] = default_dialect

        probe = "__eewpw_probe__"
        accepts_probe = False
        try:
            parser_probe = parser_cls({"dialect": probe})
            accepts_probe = str(getattr(parser_probe, "dialect", "")).strip().lower() == probe
        except Exception:
            accepts_probe = False

        if algo == "finder" and isinstance(finder_aliases, dict) and finder_aliases:
            aliases = {
                str(alias).strip().lower(): str(canonical).strip().lower()
                for alias, canonical in finder_aliases.items()
            }
            canonical = sorted({v for v in aliases.values() if v})
            info["accepted_literals"] = sorted(aliases.keys())
            info["canonical"] = canonical
            info["alias_map"] = aliases
        else:
            if accepts_probe:
                info["accepted_input_note"] = "parser does not restrict this value"
                if default_dialect:
                    info["canonical"] = [default_dialect]
            else:
                if default_dialect:
                    info["accepted_literals"] = [default_dialect]
                    info["canonical"] = [default_dialect]
                else:
                    info["accepted_input_note"] = "accepted dialect could not be determined"

        out[algo] = info

    return out


def _sorted_alias_pairs(alias_map: Dict[str, str]) -> List[tuple]:
    return sorted(alias_map.items(), key=lambda item: (item[1], item[0]))


def _discover_live_most_complete_algorithms() -> List[str]:
    path = Path(__file__).resolve().parent / "live_engine.py"
    if not path.exists():
        return []

    text = path.read_text(encoding="utf-8")
    m = re.search(r"self\._ann_profile\s*=\s*\{(?P<body>.*?)\}\.get", text, flags=re.DOTALL)
    if not m:
        return []

    body = m.group("body")
    algos = re.findall(r'"([a-zA-Z0-9_]+)"\s*:', body)
    seen = set()
    ordered: List[str] = []
    for algo in algos:
        if algo in seen:
            continue
        seen.add(algo)
        ordered.append(algo)
    return ordered


def _canonical_lookup_keys(capabilities: Dict[str, Dict[str, Any]]) -> List[str]:
    keys: List[str] = []
    for algo in sorted(capabilities):
        canonical = capabilities[algo].get("canonical", [])
        for dialect in canonical:
            lookup_key = parser_config._normalize_annotation_lookup_key(algo, dialect)
            if lookup_key:
                keys.append(lookup_key)
    return list(dict.fromkeys(keys))


def _discover_legacy_profile_by_key(capabilities: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}

    legacy_ctx = getattr(parser_config, "_LEGACY_PROFILE_ANNOTATION_CONTEXT", {})
    if isinstance(legacy_ctx, dict):
        for rel_path, triple in legacy_ctx.items():
            if not isinstance(triple, tuple) or len(triple) != 3:
                continue
            target, algo, dialect = triple
            if target != _ANNOTATION_TARGET:
                continue
            lookup_key = parser_config._normalize_annotation_lookup_key(algo, dialect)
            if lookup_key:
                out[lookup_key] = rel_path

    try:
        finder_dialects = import_module("eewpw_parser.parsers.finder.dialects")
        module_name = finder_dialects.__name__
        for _, cls in inspect.getmembers(finder_dialects, inspect.isclass):
            if cls.__module__ != module_name:
                continue
            profile_name = getattr(cls, "PROFILE_NAME", None)
            lookup_algo = getattr(cls, "PROFILE_LOOKUP_ALGO", "finder")
            lookup_dialect = getattr(cls, "PROFILE_LOOKUP_DIALECT", None)
            if not isinstance(profile_name, str) or not isinstance(lookup_dialect, str):
                continue
            lookup_key = parser_config._normalize_annotation_lookup_key(lookup_algo, lookup_dialect)
            if lookup_key:
                out[lookup_key] = profile_name
    except Exception:
        pass

    for algo, info in capabilities.items():
        profile_name = info.get("default_profile")
        if not isinstance(profile_name, str):
            continue
        for dialect in info.get("canonical", []):
            lookup_key = parser_config._normalize_annotation_lookup_key(algo, dialect)
            if lookup_key and lookup_key not in out:
                out[lookup_key] = profile_name

    return out


def _load_annotations_target_state() -> Dict[str, Any]:
    winner, _ = config_loader.get_config_path_resolution("annotations.json")
    state: Dict[str, Any] = {
        "winner": winner,
        "reason": None,
        "target_cfg": None,
        "active_keys": [],
    }

    if winner is None:
        state["reason"] = "annotations.json missing"
        return state

    try:
        raw = json.loads(Path(winner).read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        state["reason"] = "annotations.json invalid JSON"
        return state
    except OSError:
        state["reason"] = "annotations.json unreadable"
        return state

    annotations = raw.get("annotations")
    if not isinstance(annotations, dict):
        state["reason"] = "annotations.json does not define an object at 'annotations'"
        return state

    target_cfg = annotations.get(_ANNOTATION_TARGET)
    if not isinstance(target_cfg, dict):
        state["reason"] = f"target '{_ANNOTATION_TARGET}' is not defined"
        return state

    state["target_cfg"] = target_cfg
    state["active_keys"] = sorted(
        key for key, value in target_cfg.items() if isinstance(value, dict)
    )
    return state


def _resolve_annotation_source_for_key(
    lookup_key: str,
    annotations_state: Dict[str, Any],
    legacy_profile_by_key: Dict[str, str],
) -> Dict[str, Any]:
    winner = annotations_state.get("winner")
    target_cfg = annotations_state.get("target_cfg")
    reason = annotations_state.get("reason")

    if isinstance(target_cfg, dict):
        patterns = target_cfg.get(lookup_key)
        if isinstance(patterns, dict):
            return {
                "lookup_key": lookup_key,
                "source": "annotations.json",
                "reason": "key exists in annotations.json",
                "winner": winner,
                "legacy_rel_path": None,
            }
        if lookup_key in target_cfg:
            miss_reason = f"key '{lookup_key}' is not an object in annotations.json"
        else:
            miss_reason = f"key '{lookup_key}' not defined in annotations.json"
    else:
        miss_reason = reason or "annotations target unavailable"

    legacy_rel_path = legacy_profile_by_key.get(lookup_key)
    if legacy_rel_path:
        legacy_winner, _ = config_loader.get_config_path_resolution(legacy_rel_path)
        if legacy_winner is not None:
            return {
                "lookup_key": lookup_key,
                "source": "legacy profile",
                "reason": miss_reason,
                "winner": legacy_winner,
                "legacy_rel_path": legacy_rel_path,
            }
        return {
            "lookup_key": lookup_key,
            "source": "unresolved",
            "reason": f"{miss_reason}; legacy profile missing: {legacy_rel_path}",
            "winner": None,
            "legacy_rel_path": legacy_rel_path,
        }

    return {
        "lookup_key": lookup_key,
        "source": "unresolved",
        "reason": f"{miss_reason}; no known legacy profile mapping",
        "winner": None,
        "legacy_rel_path": None,
    }


def _render_path(path: Optional[Path]) -> str:
    if path is None:
        return "none"
    return str(path.expanduser().resolve(strict=False))


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

    capabilities = _build_algo_capabilities()
    canonical_keys = _canonical_lookup_keys(capabilities)
    legacy_profile_by_key = _discover_legacy_profile_by_key(capabilities)
    annotations_state = _load_annotations_target_state()
    live_most_complete_algos = _discover_live_most_complete_algorithms()

    lines.append("Supported algorithms and dialects")
    lines.append("-" * 32)
    for algo in sorted(capabilities):
        info = capabilities[algo]
        lines.append(algo)
        alias_map = info.get("alias_map") or {}
        canonical = info.get("canonical") or []

        if algo == "finder" and alias_map:
            if canonical:
                lines.append(f"  canonical dialects: {', '.join(canonical)}")
            else:
                lines.append("  canonical dialects: (none)")
            lines.append("  accepted inputs (alias -> canonical):")
            for alias, canonical_dialect in _sorted_alias_pairs(alias_map):
                alias_label = f"    {alias} "
                lines.append(f"{alias_label:.<28} -> {canonical_dialect}")
        else:
            accepted_literals = info.get("accepted_literals") or []
            accepted_input_note = info.get("accepted_input_note")
            if accepted_literals:
                lines.append(f"  accepted dialects: {', '.join(accepted_literals)}")
            elif accepted_input_note:
                lines.append(f"  accepted input : {accepted_input_note}")
            else:
                lines.append("  accepted input : (undetermined)")

            if canonical:
                lines.append(f"  canonical dialects: {', '.join(canonical)}")
            else:
                lines.append("  canonical dialects: (none)")

        lines.append("")

    live_most_complete_keys: List[str] = []
    for algo in live_most_complete_algos:
        info = capabilities.get(algo)
        if not info:
            continue
        dialect = info.get("default_dialect")
        if not dialect:
            canonical = info.get("canonical") or []
            dialect = canonical[0] if canonical else None
        if not dialect:
            continue
        lookup_key = parser_config._normalize_annotation_lookup_key(algo, dialect)
        if lookup_key:
            live_most_complete_keys.append(lookup_key)

    live_most_complete_keys = list(dict.fromkeys(live_most_complete_keys))
    offline_only_keys = [k for k in canonical_keys if k not in set(live_most_complete_keys)]

    # Uncomment the live-mode support section when we have more to say about it.
    # For now, we keep it commented out to have a cleaner report.
    # lines.append("Live-mode support")
    # lines.append("-" * 32)
    # if live_most_complete_keys:
    #     lines.append(f"  most-complete combos: {', '.join(live_most_complete_keys)}")
    # else:
    #     lines.append("  most-complete combos: (none discovered)")
    # if offline_only_keys:
    #     lines.append(f"  offline-only by design: {', '.join(offline_only_keys)}")
    # else:
    #     lines.append("  offline-only by design: (none)")
    # lines.append("")

    lines.append("annotations.json active keys")
    lines.append("-" * 32)
    lines.append(f"  winner: {_render_path(annotations_state.get('winner'))}")
    target_cfg = annotations_state.get("target_cfg")
    if isinstance(target_cfg, dict):
        active_keys = annotations_state.get("active_keys", [])
        lines.append(f"  target '{_ANNOTATION_TARGET}' keys ({len(active_keys)}):")
        for key in active_keys:
            lines.append(f"  - {key}")
    else:
        lines.append(f"  target '{_ANNOTATION_TARGET}': unavailable ({annotations_state.get('reason')})")
    lines.append("")

    lines.append("Annotation resolution report")
    lines.append("-" * 32)
    resolution_rows: List[Dict[str, Any]] = []
    for lookup_key in canonical_keys:
        row = _resolve_annotation_source_for_key(
            lookup_key=lookup_key,
            annotations_state=annotations_state,
            legacy_profile_by_key=legacy_profile_by_key,
        )
        resolution_rows.append(row)
        lines.append(
            f"  {lookup_key:<28} : {row['source']} "
            f"(reason: {row['reason']})"
        )
    if not resolution_rows:
        lines.append("  (no canonical algorithm/dialect combinations discovered)")
    lines.append("")

    used_legacy_profiles = sorted(
        {
            row.get("legacy_rel_path")
            for row in resolution_rows
            if row.get("source") == "legacy profile" and row.get("legacy_rel_path")
        }
    )
    shipped_profiles = _packaged_profile_paths()
    unused_shipped_profiles = sorted(
        rel_path for rel_path in shipped_profiles if rel_path not in set(used_legacy_profiles)
    )

    # These are the old profile JSON files that we still support.
    lines.append("Deprecated legacy profile usage")
    lines.append("-" * 32)
    if used_legacy_profiles:
        for rel_path in used_legacy_profiles:
            winner, _ = config_loader.get_config_path_resolution(rel_path)
            lines.append(f"  [used fallback] {rel_path}")
    else:
        lines.append("  [used fallback] none")
    if unused_shipped_profiles:
        for rel_path in unused_shipped_profiles:
            winner, _ = config_loader.get_config_path_resolution(rel_path)
            lines.append(f"  [unused shipped] {rel_path}")
    else:
        lines.append("  [unused shipped] none")
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
