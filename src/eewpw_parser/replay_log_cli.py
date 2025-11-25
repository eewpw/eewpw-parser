import argparse
import sys
import time
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Iterable, List
from dateutil import parser as dtp


P_PREFIX = re.compile(
    r"^("
    r"\d{4}[/-]\d{2}[/-]\d{2}"          # date
    r"[ T]\d{2}:\d{2}:\d{2}"            # time
    r"(?:[:.]\d{1,6})?"                 # optional fractional
    r")"
)
P_INLINE = re.compile(
    r"("
    r"\d{4}[/-]\d{2}[/-]\d{2}[ ,T]\d{2}:\d{2}:\d{2}"
    r"(?:[.:]\d{1,6})?"
    r")"
)


def extract_timestamp(line: str) -> Optional[datetime]:
    """Extract a timestamp from the line using common Finder/VS patterns or a flexible parse."""
    candidates: List[str] = []

    # Prefix match (FinDer-like / VS-like)
    m_prefix = P_PREFIX.match(line)
    if m_prefix:
        candidates.append(m_prefix.group(1))

    # Inline match
    m_inline = P_INLINE.search(line)
    if m_inline:
        candidates.append(m_inline.group(1))

    # Try candidates first
    for cand in candidates:
        try:
            dt = dtp.parse(cand)
            break
        except Exception:
            dt = None
    else:
        # Fallback: parse entire line
        try:
            dt = dtp.parse(line)
        except Exception:
            dt = None

    if dt is None:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def find_earliest_timestamp_for_file(src_path: Path) -> Optional[datetime]:
    """Return the first timestamp found in a file, or None if none exist."""
    with src_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            ts = extract_timestamp(line)
            if ts is not None:
                return ts
    return None


def compute_sleep_seconds(
    prev_ts: Optional[datetime],
    curr_ts: Optional[datetime],
    global_min_ts: Optional[datetime],
    speed: float,
) -> float:
    """Compute sleep time based on global baseline and speed factor."""
    if speed <= 0:
        speed = 1.0
    elif speed < 0.001:
        speed = 0.001

    if global_min_ts is None:
        return 0.0

    if curr_ts is None:
        if prev_ts is None:
            return 0.0
        curr_ts = prev_ts

    curr_offset = max(0.0, (curr_ts - global_min_ts).total_seconds())

    if prev_ts is None:
        delta = curr_offset
    else:
        prev_offset = max(0.0, (prev_ts - global_min_ts).total_seconds())
        delta = max(0.0, curr_offset - prev_offset)

    return delta / speed


def ensure_tmp_and_target(src_path: Path) -> Path:
    """Create ./tmp and truncate the target fake log before writing."""
    tmp_dir = Path("tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    dst_path = tmp_dir / f"fake_{src_path.name}"
    dst_path.write_text("", encoding="utf-8")
    return dst_path


def playback_file(
    src_path: Path,
    dst_path: Path,
    global_min_ts: Optional[datetime],
    speed: float,
) -> None:
    """Replay a single file into its fake target with timing based on extracted timestamps."""
    # First, count total number of lines for progress reporting.
    try:
        with src_path.open("r", encoding="utf-8", errors="ignore") as f_count:
            total_lines = sum(1 for _ in f_count)
    except Exception:
        total_lines = None

    prev_ts = None
    processed = 0

    with src_path.open("r", encoding="utf-8", errors="ignore") as f_in, dst_path.open(
        "a", encoding="utf-8"
    ) as f_out:
        for line in f_in:
            curr_ts = extract_timestamp(line)
            sleep_secs = compute_sleep_seconds(prev_ts, curr_ts, global_min_ts, speed)

            processed += 1
            if total_lines is not None and total_lines > 0:
                msg = f"[replay:{src_path.name}] {processed}/{total_lines} lines (next in {sleep_secs:.3f}s)"
            else:
                msg = f"[replay:{src_path.name}] {processed} lines (next in {sleep_secs:.3f}s)"
            print("\r" + msg, end="", flush=True)

            if sleep_secs > 0:
                time.sleep(sleep_secs)

            f_out.write(line)
            f_out.flush()

            if curr_ts is not None:
                prev_ts = curr_ts

    print()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="EEWPW raw log replay (writes fake logs under ./tmp)")
    ap.add_argument("--speed", type=float, default=1.0, help="Replay speed factor (<=0 disables sleeping)")
    ap.add_argument(
        "--file-list",
        dest="file_list",
        default=None,
        help="Path to a file containing log paths (one per line, # comments allowed)",
    )
    ap.add_argument("-v", "--verbose", action="store_true", help="Enable verbose replay output")
    ap.add_argument("inputs", nargs="*", help="Log files to replay sequentially")
    return ap.parse_args()


def read_paths_from_file_list(file_list_path: Path) -> List[Path]:
    paths: List[Path] = []
    with file_list_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            paths.append(Path(s))
    return paths


def read_paths_from_stdin() -> List[Path]:
    paths: List[Path] = []
    for line in sys.stdin:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        paths.append(Path(s))
    return paths


def collect_input_paths(positional: List[str], file_list: Optional[str]) -> List[Path]:
    collected: List[Path] = []
    seen = set()

    def add_path(p: Path):
        key = str(p)
        if key in seen:
            return
        seen.add(key)
        collected.append(p)

    for p in positional:
        add_path(Path(p))

    if file_list is not None:
        for p in read_paths_from_file_list(Path(file_list)):
            add_path(p)

    if not positional and file_list is None and not sys.stdin.isatty():
        for p in read_paths_from_stdin():
            add_path(p)

    if not collected:
        print("No input log paths provided.", file=sys.stderr)
        sys.exit(1)

    for p in collected:
        if not p.is_file():
            print(f"Input path is not a regular file: {p}", file=sys.stderr)
            sys.exit(1)

    return collected


def find_earliest_ts(path: Path) -> Optional[float]:
    earliest: Optional[float] = None
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            ts = extract_timestamp(line)
            if ts is None:
                continue
            if earliest is None or ts < earliest:
                earliest = ts
    return earliest


def main() -> int:
    try:
        args = parse_args()
        paths = collect_input_paths(args.inputs, args.file_list)

        earliest_list = [find_earliest_timestamp_for_file(p) for p in paths]
        global_min_ts_candidates = [ts for ts in earliest_list if ts is not None]
        global_min_ts = min(global_min_ts_candidates) if global_min_ts_candidates else None

        # Sort files: those with timestamps ordered by earliest ts; those without timestamps retain relative order at end.
        sorted_paths = list(paths)
        if global_min_ts is not None:
            ts_with_idx = []
            ts_none = []
            for idx, (p, ts) in enumerate(zip(paths, earliest_list)):
                if ts is None:
                    ts_none.append((idx, p))
                else:
                    ts_with_idx.append((ts, idx, p))
            ts_with_idx.sort(key=lambda x: x[0])
            ts_none.sort(key=lambda x: x[0])
            sorted_paths = [p for _, _, p in ts_with_idx] + [p for _, p in ts_none]

        for src_path in sorted_paths:
            dst_path = ensure_tmp_and_target(src_path)
            playback_file(src_path, dst_path, global_min_ts, args.speed)
            if args.verbose:
                print(f"[replay] finished {src_path.name} -> {dst_path}", flush=True)

        if args.verbose:
            print(f"[replay] Finished writing fake logs under tmp", flush=True)
        return 0
    except SystemExit as e:
        return int(getattr(e, "code", 1) or 1)
    except Exception as exc:
        print(f"Replay failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
