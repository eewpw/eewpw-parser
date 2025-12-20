import json
import argparse
from pathlib import Path
from eewpw_parser.config import load_global_config
from eewpw_parser.config_loader import set_config_root_override
from eewpw_parser.parsers.finder.finder_parser import FinderParser
from eewpw_parser.parsers.vs.vs_parser import VSParser
from eewpw_parser.schemas import FinalDoc

def main():
    ap = argparse.ArgumentParser(description="EEWPW deterministic parser")
    ap.add_argument("--algo", required=True, choices=["finder", "vs"], help="Algorithm to parse")
    ap.add_argument("--dialect", required=True, help="Dialect (e.g., scfinder)")
    ap.add_argument("--config-root", type=Path, default=None, help="Optional override for configs root")
    ap.add_argument("-v", "--verbose", action="store_true", help="Enable verbose console output")
    ap.add_argument(
        "--mode",
        choices=["batch", "stream-jsonl"],
        default="batch",
        help="Output mode: batch returns a JSON FinalDoc; stream-jsonl writes JSONL records (detection/annotation/meta).",
    )
    ap.add_argument(
        "--instance",
        default=None,
        help="Instance identifier (e.g., finder@host1); defaults to '<algo>@unknown' if not provided.",
    )
    ap.add_argument("-o", "--output", required=True, help="Output JSON file")
    ap.add_argument("inputs", nargs="+", help="One or more input log files (top-level only)")
    args = ap.parse_args()

    if args.config_root is not None:
        set_config_root_override(args.config_root)

    cfg = load_global_config()
    cfg["algo"] = args.algo
    cfg["dialect"] = args.dialect
    cfg["verbose"] = args.verbose
    instance = args.instance or f"{args.algo}@unknown"

    if args.verbose:
        print(f"EEWPW Parser start: algo={args.algo} dialect={cfg.get('dialect')} files={len(args.inputs)}")

    if args.algo == "finder":
        parser = FinderParser(cfg)
    elif args.algo == "vs":
        parser = VSParser(cfg)
    else:
        raise SystemExit(f"Unsupported algo: {args.algo}")

    if args.mode == "batch":
        doc: FinalDoc = parser.parse(args.inputs)
        pretty = bool(cfg.get("output", {}).get("pretty", True))
        indent = int(cfg.get("output", {}).get("indent", 2))
        ensure_ascii = bool(cfg.get("output", {}).get("ensure_ascii", False))

        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(doc.dict(), f, indent=indent if pretty else None, ensure_ascii=ensure_ascii)
        print(f"Wrote {args.output}")
    elif args.mode == "stream-jsonl":
        from eewpw_parser.sinks import JsonlStreamSink

        out_path = Path(args.output)
        sink = JsonlStreamSink(out_path, algo=args.algo, dialect=cfg.get("dialect"), instance=instance)
        parser.parse(args.inputs, sink=sink)
        print(f"Streamed JSONL to {out_path}")
    else:
        raise SystemExit(f"Unsupported mode: {args.mode}")


if __name__ == "__main__":
    main()
    
