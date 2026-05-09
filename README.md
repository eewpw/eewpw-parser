# eewpw-parser

Deterministic parsers for EEW (Earthquake Early Warning) algorithm logs. `eewpw-parse` is the offline parser and writes canonical JSON by default. `eewpw-parse-live` is the dedicated live tail command and writes daily JSONL files.

## Supported algorithms

Implemented parser families:
- `finder`
- `vs`
- `plum`
- `epic`
- `gfast`
- `eqinfo`

Live mode (`eewpw-parse-live`) currently supports only:
- `finder`
- `vs`
- `plum`: not fully tested
- `epic`: not fully tested

`gfast` and `eqinfo` are not supported in live mode. 
`plum` and `epic` are not fully tested. 
We plan to support all algorithms in the live-mode in the future. 

## Installation

```bash
pip install -e .
```

Requires Python 3.9+. Runtime deps include `pydantic>=2,<3` and `python-dateutil`.

## Quick start

Batch JSON output (default mode):

```bash
eewpw-parse --algo finder --dialect scfinder -o out.json /path/to/log1 /path/to/log2
```

Legacy offline JSONL output (deprecated):

```bash
eewpw-parse --algo vs --dialect scvsmag --mode stream-jsonl --instance vs@node1 -o out.jsonl /path/to/scvsmag-processing-info.log
```

Live tailing (`--data-root` preferred):

```bash
eewpw-parse-live --algo plum --dialect plum --logfile /path/to/live.log --data-root ./data --instance plum@node1
```

Replay raw logs:

```bash
eewpw-replay-log --speed 10.0 /path/to/A.log /path/to/B.log
```

## Configuration

- Use `--config-root` to select an explicit config root.
- If unset, `EEWPW_PARSER_CONFIG_ROOT` is used when present.
- Otherwise packaged defaults under `src/eewpw_parser/configs/` are used.
- There is no automatic repo-root fallback; `example-configs/` is example material only unless explicitly passed as a config root.
- Put custom annotation patterns in `annotations.json` under your config root and pass that root with `--config-root`.
- `profiles/*.json` is still supported as fallback for compatibility, but is deprecated and planned for removal.
- Use `eewpw-parse --show-env` to inspect the current runtime environment and see which configuration files are selected by the parser.

## Documentation

- Entry points (summary):
  - `eewpw-parse`: offline parser CLI (default canonical JSON; `--mode stream-jsonl` is deprecated)
  - `eewpw-parse-live`: live tailing to daily JSONL raw files
  - `eewpw-replay-log`: raw log replay helper for live-style testing

- [Parser usage overview](docs/parser-usage.md)
- [Live parsing guide (`eewpw-parse-live`)](docs/live-parsing.md)
- [Log replay guide (`eewpw-replay-log`)](docs/log-replay.md)
- [Architecture](docs/architecture.md)
- [Finder parser notes](docs/parsers_finder.md)
- [VS parser notes](docs/parsers_vs.md)
