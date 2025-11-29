import argparse
import sys
import time
import re
from datetime import datetime, timezone, timedelta
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



def format_timestamp_like(original: str, new_ts: datetime) -> str:
    """
    Format new_ts using the same structural format as the original timestamp string.

    We preserve:
    - date separator: '-' vs '/'
    - separator between date and time: ' ', 'T', or ','
    - presence and length of fractional seconds
    - fractional separator: '.' vs ':'
    """
    m = re.match(
        r"(?P<date>\d{4}[/-]\d{2}[/-]\d{2})"
        r"(?P<dtsep>[ T,])"
        r"(?P<hms>\d{2}:\d{2}:\d{2})"
        r"(?:(?P<fracsep>[.:])(?P<frac>\d{1,6}))?",
        original,
    )
    if not m:
        # Fallback: keep current generic formatting if we cannot parse the pattern
        return new_ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    date_str = m.group("date")
    dtsep = m.group("dtsep")
    fracsep = m.group("fracsep")
    frac = m.group("frac")

    # Determine date separator from the original (position 4 is safe for YYYY?MM?DD)
    date_sep = date_str[4] if len(date_str) >= 5 else "-"

    # Build date and time with appropriate separators
    date_out = f"{new_ts.year:04d}{date_sep}{new_ts.month:02d}{date_sep}{new_ts.day:02d}"
    time_out = f"{new_ts.hour:02d}:{new_ts.minute:02d}:{new_ts.second:02d}"

    if frac is not None and fracsep is not None:
        frac_len = len(frac)
        micro_str = f"{new_ts.microsecond:06d}"
        frac_out = micro_str[:frac_len]
        return f"{date_out}{dtsep}{time_out}{fracsep}{frac_out}"

    return f"{date_out}{dtsep}{time_out}"

def rewrite_timestamp_in_line(line: str, new_ts: datetime) -> str:
    # Try prefix-style timestamps first
    m_prefix = P_PREFIX.match(line)
    if m_prefix:
        original_ts = m_prefix.group(1)
        ts_str = format_timestamp_like(original_ts, new_ts)
        start, end = m_prefix.span(1)
        return line[:start] + ts_str + line[end:]

    # Then try inline-style timestamps
    m_inline = P_INLINE.search(line)
    if m_inline:
        original_ts = m_inline.group(1)
        ts_str = format_timestamp_like(original_ts, new_ts)
        start, end = m_inline.span(1)
        return line[:start] + ts_str + line[end:]

    # If we can't find a recognizable timestamp, leave the line unchanged
    return line


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
    repeat: int = 1,
) -> None:
    """Replay a single file into its fake target with timing based on extracted timestamps."""
    try:
        entries: List[tuple[str, Optional[datetime]]] = []
        earliest_ts: Optional[datetime] = None
        latest_ts: Optional[datetime] = None
        with src_path.open("r", encoding="utf-8", errors="ignore") as f_in:
            for line in f_in:
                ts = extract_timestamp(line)
                entries.append((line, ts))
                if ts is not None:
                    if earliest_ts is None or ts < earliest_ts:
                        earliest_ts = ts
                    if latest_ts is None or ts > latest_ts:
                        latest_ts = ts
        total_lines = len(entries)
    except Exception:
        entries = []
        total_lines = None

    if earliest_ts is not None and latest_ts is not None and latest_ts > earliest_ts:
        per_cycle_offset_seconds = (latest_ts - earliest_ts).total_seconds()
    elif earliest_ts is not None or latest_ts is not None:
        per_cycle_offset_seconds = 60.0
    else:
        per_cycle_offset_seconds = None

    total_expected = total_lines * max(1, repeat) if total_lines is not None else None

    prev_ts: Optional[datetime] = None
    processed = 0

    with dst_path.open("a", encoding="utf-8") as f_out:
        for rep in range(max(1, repeat)):
            for original_line, original_ts in entries:
                if original_ts is not None and per_cycle_offset_seconds is not None:
                    offset_seconds = per_cycle_offset_seconds * rep
                    new_ts = original_ts + timedelta(seconds=offset_seconds)
                else:
                    new_ts = original_ts

                if new_ts is not None:
                    sleep_secs = compute_sleep_seconds(prev_ts, new_ts, global_min_ts, speed)
                else:
                    sleep_secs = compute_sleep_seconds(prev_ts, prev_ts, global_min_ts, speed)

                processed += 1
                if total_expected is not None and total_expected > 0:
                    msg = f"[replay:{src_path.name}] {processed}/{total_expected} lines (next in {sleep_secs:.3f}s)"
                else:
                    msg = f"[replay:{src_path.name}] {processed} lines (next in {sleep_secs:.3f}s)"
                print("\r" + msg, end="", flush=True)

                if sleep_secs > 0:
                    time.sleep(sleep_secs)

                if new_ts is not None:
                    line_to_write = rewrite_timestamp_in_line(original_line, new_ts)
                else:
                    line_to_write = original_line

                f_out.write(line_to_write)
                f_out.flush()

                if new_ts is not None:
                    prev_ts = new_ts
                elif original_ts is not None:
                    prev_ts = original_ts

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
    ap.add_argument("--repeat", type=int, default=1, help="Repeat each input log N times in the fake output (timestamps adjusted when possible)")
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
            playback_file(src_path, dst_path, global_min_ts, args.speed, repeat=args.repeat)
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
