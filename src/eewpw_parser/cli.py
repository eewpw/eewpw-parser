import json
import argparse
from eewpw_parser.config import load_config
from eewpw_parser.parsers.finder.finder_parser import FinderParser
from eewpw_parser.parsers.vs.vs_parser import VSParser
from eewpw_parser.schemas import FinalDoc

def main():
    ap = argparse.ArgumentParser(description="EEWPW deterministic parser")
    ap.add_argument("--algo", required=True, choices=["finder", "vs"], help="Algorithm to parse")
    ap.add_argument("--dialect", default=None, help="Dialect (e.g., scfinder)")
    ap.add_argument("-v", "--verbose", action="store_true", help="Enable verbose console output")
    ap.add_argument("-o", "--output", required=True, help="Output JSON file")
    ap.add_argument("inputs", nargs="+", help="One or more input log files (top-level only)")
    args = ap.parse_args()

    cfg_path = "configs/finder.json" if args.algo == "finder" else "configs/vs.json"
    cfg = load_config(cfg_path)
    if args.dialect:
        cfg["dialect"] = args.dialect
    cfg["verbose"] = args.verbose

    if args.verbose:
        print(f"EEWPW Parser start: algo={args.algo} dialect={cfg.get('dialect')} files={len(args.inputs)}")

    if args.algo == "finder":
        parser = FinderParser(cfg)
    elif args.algo == "vs":
        parser = VSParser(cfg)
    else:
        raise SystemExit(f"Unsupported algo: {args.algo}")

    doc: FinalDoc = parser.parse(args.inputs)

    pretty = bool(cfg.get("output", {}).get("pretty", True))
    indent = int(cfg.get("output", {}).get("indent", 2))
    ensure_ascii = bool(cfg.get("output", {}).get("ensure_ascii", False))

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(doc.dict(), f, indent=indent if pretty else None, ensure_ascii=ensure_ascii)

    print(f"Wrote {args.output}")

if __name__ == "__main__":
    main()
    
