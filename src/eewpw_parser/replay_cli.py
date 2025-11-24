# -*- coding: utf-8 -*-
import argparse
import time
from pathlib import Path
from typing import Optional
from dateutil import parser as dtp

from eewpw_parser.sinks import JsonlStreamSink, BaseSink
from eewpw_parser.parsers.finder.finder_parser import FinderParser
from eewpw_parser.parsers.vs.vs_parser import VSParser
from eewpw_parser.sources import ReplayLineSource


def _iter_lines_for_replay(paths: list[str]):
    """
    Centralized file iterator for replay; currently uses ReplayLineSource.
    """
    source = ReplayLineSource(paths)
    return source.iterate_files()


class SleepSink(BaseSink):
    def __init__(self, inner: BaseSink, speed: float, verbose: bool = False):
        self.inner = inner
        self.speed = speed
        self.verbose = verbose
        self._last_ts = None

    def start_run(self) -> None:
        self.inner.start_run()

    def _maybe_sleep(self, ts_str: Optional[str]) -> None:
        if not ts_str or self.speed <= 0:
            return
        try:
            ts = dtp.parse(ts_str)
        except Exception:
            return
        if self._last_ts is None:
            self._last_ts = ts
            return
        delta = (ts - self._last_ts).total_seconds()
        if delta > 0:
            if self.verbose:
                print(f"[replay] sleeping {delta/self.speed:.3f}s (real Δ={delta:.3f}s)", flush=True)
            time.sleep(delta / self.speed)
        self._last_ts = ts

    def emit_detection(self, det):
        self._maybe_sleep(det.timestamp)
        self.inner.emit_detection(det)

    def emit_annotation(self, profile: str, ann):
        self._maybe_sleep(ann.timestamp)
        self.inner.emit_annotation(profile, ann)

    def finalize(self, meta):
        meta.extras = meta.extras or {}
        meta.extras.setdefault(
            "replay",
            {
                "speed": self.speed,
                "note": "sequential replay",
            },
        )
        return self.inner.finalize(meta)


def main():
    ap = argparse.ArgumentParser(description="EEWPW replay parser (streams JSONL with timing)")
    ap.add_argument("--algo", required=True, choices=["finder", "vs"], help="Algorithm to parse")
    ap.add_argument("--dialect", default=None, help="Dialect (e.g., scfinder or scvs)")
    ap.add_argument("--instance", default=None, help="Instance id (default '<algo>@replay')")
    ap.add_argument("--speed", type=float, default=1.0, help="Replay speed factor (<=0 disables sleeping)")
    ap.add_argument("-v", "--verbose", action="store_true", help="Enable verbose replay output")
    ap.add_argument("-o", "--output", required=True, help="Output JSONL file path")
    ap.add_argument("inputs", nargs="+", help="Log files to replay sequentially")
    args = ap.parse_args()

    dialect = args.dialect or ("scfinder" if args.algo == "finder" else "scvs")
    instance = args.instance or f"{args.algo}@replay"

    if args.algo == "finder":
        parser = FinderParser({"dialect": dialect})
    elif args.algo == "vs":
        parser = VSParser({"dialect": dialect})
    else:
        raise SystemExit(f"Unsupported algo: {args.algo}")

    out_path = Path(args.output)
    base_sink = JsonlStreamSink(out_path, algo=args.algo, dialect=dialect, instance=instance, verbose=args.verbose)
    sink = SleepSink(base_sink, speed=args.speed, verbose=args.verbose)

    # Replay currently reuses parser path-based processing; iteration is centralized for future tailing.
    parser.parse(args.inputs, sink=sink)
    print(f"Replayed logs to JSONL: {out_path} (mode=stream, speed={args.speed})")


if __name__ == "__main__":
    main()
