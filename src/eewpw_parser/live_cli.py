# -*- coding: utf-8 -*-
import argparse
from pathlib import Path

from eewpw_parser.config import load_config
from eewpw_parser.sources import TailLineSource
from eewpw_parser.live_engine import LiveEngine
from eewpw_parser.parsers.finder.finder_parser import FinderParser
from eewpw_parser.parsers.vs.dialects import VSDialect


def main():
    ap = argparse.ArgumentParser(description="EEWPW live parser (tail + stream per-event JSONL)")
    ap.add_argument("--algo", required=True, choices=["finder", "vs"], help="Algorithm to parse")
    ap.add_argument("--dialect", default=None, help="Dialect (e.g., scfinder, scvs)")
    ap.add_argument("--instance", default=None, help="Instance identifier (e.g., finder@host1)")
    ap.add_argument("--logfile", required=True, help="Path to log file to tail")
    ap.add_argument("--output-dir", required=True, help="Directory to write per-event JSONL files")
    ap.add_argument("--verbose", action="store_true", help="Enable verbose output")
    ap.add_argument("--poll-interval", type=float, default=0.1, help="Polling interval for tailing in seconds")
    args = ap.parse_args()

    cfg_path = "configs/finder.json" if args.algo == "finder" else "configs/vs.json"
    cfg = load_config(cfg_path)
    if args.dialect:
        cfg["dialect"] = args.dialect
    cfg["verbose"] = args.verbose

    instance = args.instance or f"{args.algo}@unknown"
    dialect = cfg.get("dialect")

    source = TailLineSource(
        path=str(args.logfile),
        poll_interval=float(args.poll_interval),
        seek_end=True,
        max_lines=None,
        follow=True,
    )

    if args.algo == "finder":
        parser = FinderParser(cfg)
    elif args.algo == "vs":
        worker = VSDialect()
        if hasattr(worker, "verbose"):
            worker.verbose = bool(args.verbose)
        parser = worker
    else:
        raise SystemExit(f"Unsupported algo: {args.algo}")

    engine = LiveEngine(
        source=source,
        parser=parser,
        output_dir=Path(args.output_dir),
        algo=args.algo,
        dialect=dialect,
        instance=instance,
        verbose=bool(args.verbose),
    )

    try:
        engine.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        engine.shutdown()


if __name__ == "__main__":
    main()
