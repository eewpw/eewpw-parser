#!/usr/bin/env python3
"""
Cleanup utility for live raw JSONL outputs.
Deletes daily files older than the configured retention window.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean up live/raw/<algo>/YYYY-MM-DD_<algo>.jsonl files")
    parser.add_argument("--data-root", required=True, help="Root directory containing live/raw/<algo>/ daily files")
    parser.add_argument(
        "--retention-days",
        type=int,
        default=2,
        help="Number of most recent days to keep (inclusive of today). Older files are deleted.",
    )
    return parser.parse_args()


def _date_from_filename(path: Path) -> date | None:
    try:
        date_part = path.stem.split("_", 1)[0]
        return datetime.strptime(date_part, "%Y-%m-%d").date()
    except Exception:
        return None


def cleanup_live_raw(data_root: Path, retention_days: int) -> List[Path]:
    """
    Delete files older than retention_days (inclusive of today).
    Returns list of removed file paths.
    """
    if retention_days <= 0:
        retention_days = 1
    cutoff_date = date.today() - timedelta(days=retention_days - 1)
    root = Path(data_root) / "live" / "raw"
    removed: List[Path] = []
    if not root.exists():
        return removed

    for path in root.glob("*/*.jsonl"):
        file_date = _date_from_filename(path)
        if file_date is None:
            continue
        if file_date < cutoff_date:
            try:
                path.unlink()
                removed.append(path)
            except FileNotFoundError:
                continue
    return removed


def main() -> int:
    args = _parse_args()
    try:
        cleanup_live_raw(Path(args.data_root), args.retention_days)
    except Exception as exc:
        print(f"[cleanup-live-raw] failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
