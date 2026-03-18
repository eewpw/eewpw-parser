# Parser Usage and Dialects

This document summarizes the parsers, dialects, aliases, and command-line usage currently implemented in `eewpw-parser`.

It is intended as a practical reference for:
- choosing the correct `--algo` and `--dialect`
- understanding which dialect names are canonical versus aliases
- running the parser in batch or JSONL streaming mode
- using the dedicated live and replay entry points

## Overview

The repository currently implements these parser families:  
`finder`, `vs`, `plum`, `epic`, `gfast`, `eqinfo`

In the CLI, the algorithm is selected with `--algo` and the dialect with `--dialect`.

Some algorithms expose one canonical dialect only. Finder is the main exception: it supports multiple canonical dialects and several alias spellings for backward compatibility.


| Algorithm | Dialect | Aliases | Parser |
|---|---|---|---|
| **finder** | `native_finder` | `native-finder`, `nativefinder`, `finder` | `FinderParser` |
|  | `scfinder` | none |  |
|  | `shakealert` | none |  |
|  | `native_finder_legacy` | `native-finder-legacy`, `nativefinderlegacy`, `finder_legacy`, `finder-legacy`, `finderlegacy` |  |
| **vs** | `scvsmag` | none | `VSParser` |
| **plum** | `plum` | none | `PlumParser` |
| **epic** | `shakealert` | none | `EpicParser` |
| **gfast** | `shakealert` | none | `GfastParser` |
| **eqinfo** | `shakealert` | none | `EqinfoParser` |

## CLI interfaces

The repository currently exposes three command-line entry points:

- `eewpw-parse`
- `eewpw-parse-live`
- `eewpw-replay-log`

### Main parser CLI

The main entry point is:

```bash
eewpw-parse
```

Alternatively, the CLI can be invoked directly through Python (useful during development when the package is not installed as a console script):

```bash
python -m src.eewpw_parser.cli \
  --algo finder \
  --dialect scfinder \
  --mode batch \
  --output tmp/offline_output/finder_scfinder.json \
  example-log-files/finder_scfinder/scfinder_Elm2020/scfinder.log
```

This invocation is functionally equivalent to `eewpw-parse` but bypasses the console entry point defined in `pyproject.toml`.

Core arguments:

| Flag / argument | Required | Default | Purpose |
|---|---|---|---|
| `--algo` | yes | none | Selects the parser family |
| `--dialect` | yes | none | Selects the dialect or accepted alias |
| `--output` | yes | none | Output path |
| `inputs` | yes | none | One or more input files |
| `--mode` | no | `batch` | Output mode (`batch` or `stream-jsonl`) |
| `--instance` | no | `<algo>@unknown (resolved at runtime)` | Optional instance label |
| `--config-root` | no | packaged defaults | Alternate parser config root |
| `--verbose` | no | `false` | Enable more verbose logging |
| `--show-env` | no | `false` | Print environment/config diagnostics and exit immediately |

Parser runtime config contract:
- Supported runtime files are `global.json` and `profiles/*.json`.
- Resolution order is `--config-root` override, then `EEWPW_PARSER_CONFIG_ROOT`, then packaged defaults in `src/eewpw_parser/configs/`.
- There is no automatic fallback to repo-root `./example-configs` or `./user-config`.
- Repo-root `./example-configs` is example-only; it is used at runtime only when explicitly selected as a config root.
- `eewpw-parse --show-env` mirrors this runtime per-file resolution and reports which source is selected for each runtime file.

### Environment diagnostics (`--show-env`)

Use `eewpw-parse --show-env` to print runtime diagnostics without parsing inputs.  
The report includes:
- Python interpreter information
- installed package location
- config lookup order and configured paths
- which runtime configuration source is selected per file (with fallback detail when needed)

```bash
eewpw-parse --show-env
```

Illustrative output snippet:

```text
Config lookup order
  1. --config-root               (not set)
  2. EEWPW_PARSER_CONFIG_ROOT    (not set)
  3. packaged defaults
     path      : /path/to/eewpw_parser/configs

Resolved files
-------------------------
global.json
[X] packaged defaults

profiles/vs_time_vs_mag.json
[X] --config-root /custom/configs/profiles/vs_time_vs_mag.json
```

## Profile JSON files

- Packaged profile JSON files live in `src/eewpw_parser/configs/profiles/`.
- Example copies are under `example-configs/profiles/`; users can copy/edit profiles and point the parser to them with `--config-root` or `EEWPW_PARSER_CONFIG_ROOT`.
- Top-level `algorithm` and `dialect` fields are informational metadata only. They are not used by parser runtime logic.
- Runtime annotation matching behavior is driven by `patterns` entries only (with `patterns.timestamp_regex` stripped by `load_profile()`).

Expected structure:

```json
{
  "patterns": {
    "<pattern_id>": "<string-or-regex searched in log lines>"
  }
}
```

Example:

```json
{
  "algorithm": "epic",
  "dialect": "shakealert",
  "patterns": {
    "start_event": "Start logging for event",
    "end_event": "End logging for event",
    "likelihood": "likelihood:"
  }
}
```

## Supported modes

The main CLI currently supports these modes:

| Mode | Output form | Intended use |
|---|---|---|
| `batch` | One final JSON document | Normal offline parsing |
| `stream-jsonl` | JSONL event stream | Streaming / live-style pipelines |

Important: `offline`, `live`, and `replay` are not `--mode` values of the main CLI. Live and replay are handled through dedicated entry points.

## Command examples

### 1. Offline-style batch parsing

Parse one or more files and emit a single final JSON document:

```bash
eewpw-parse \
  --algo finder \
  --dialect scfinder \
  --mode batch \
  --output tmp/offline_output/finder_scfinder.json \
  example-log-files/finder_scfinder/scfinder_Elm2020/scfinder.log
```

Another example using VS:

```bash
eewpw-parse \
  --algo vs \
  --dialect scvsmag \
  --mode batch \
  --output tmp/offline_output/vs_scvsmag.json \
  path/to/vs.log
```

### 2. JSONL streaming mode from the main CLI

Emit JSONL records instead of a single final JSON document:

```bash
eewpw-parse \
  --algo finder \
  --dialect scfinder \
  --mode stream-jsonl \
  --output tmp/live_output/finder_scfinder.jsonl \
  example-log-files/finder_scfinder/scfinder_Elm2020/scfinder.log
```

### 3. Dedicated live entry point

Use the dedicated live command for tailing / live ingestion workflows:

```bash
eewpw-parse-live \
  --algo finder \
  --dialect scfinder \
  --logfile path/to/live.log \
  --data-root tmp/live_output
```

Live mode currently supports `finder`, `vs`, `plum`, and `epic`. `gfast` and `eqinfo` are accepted by argument choices but rejected at runtime. Use `--data-root` as the preferred output root (`--output-dir` is a deprecated fallback).

For full live CLI behavior, flags, output layout, and caveats, see [Live parsing guide](live-parsing.md).

### 4. Dedicated replay entry point

Use the dedicated replay command to replay an existing log as if it were arriving over time:

```bash
eewpw-replay-log \
  example-log-files/finder_scfinder/scfinder_Elm2020/scfinder.log \
  --speed 1.0
```

Replay note: this command replays raw log lines only and does not read parser config/profile files.

For full replay CLI behavior, ordering model, timing rules, and caveats, see [Log replay guide](log-replay.md).

## Notes and maintenance guidance

- Prefer canonical dialect names in documentation and scripts.
- Treat alias spellings as compatibility inputs, not preferred names.
- Profile JSONs provide annotation match regex patterns; `patterns.timestamp_regex` is not a runtime key and is stripped by `load_profile()`.
- PLUM uses the shared profile loader path (`profiles/plum_time_vs_mag.json`) like the other parsers, and PLUM annotation timestamps are intentionally `""`.
- If a new dialect is added in code, this file should be updated at the same time.
- If CLI flags or entry points change, update this file together with the README examples.
