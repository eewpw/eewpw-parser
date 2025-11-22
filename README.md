# eewpw-parser

Deterministic, streaming-friendly parsers for EEW (Earthquake Early Warning) algorithm logs. Current algorithms: Finder and VS. Output is a unified JSON `FinalDoc` for downstream tooling.

## Install

```bash
pip install -e .
```

Requires Python 3.9+. Runtime deps: `pydantic<2`, `python-dateutil`.

## CLI Usage

Installed script:

```bash
eewpw-parse --algo finder --dialect scfinder -o out.json /path/to/log1 /path/to/log2
```

Dev entrypoint:

```bash
python -m eewpw_parser.cli --algo vs --dialect scvs -o out.json /path/to/scvsmag-processing-info.log
```

Notes:
- Config is merged from `configs/global.json` and `configs/<algo>.json` (e.g., `configs/finder.json`).
- Output formatting can be tuned via `output.pretty|indent|ensure_ascii` in config.
- Detections are sorted by `timestamp`; per-file extras are emitted under `meta.extras["files"]`.

## Streaming Design

- Finder: `FinderParser` orchestrates dialects in `parsers/finder/dialects.py` using `parse_stream(lines, state, finalize)`; keeps a `FinderStreamState` for partial blocks and station tables.
- VS: `VSDialect` processes lines incrementally via `feed_line(...)` + `flush(...)` and tracks a `VSEventState` per event between “Start/End logging for event”.
- Annotations use regex profiles (e.g., `configs/profiles/vs_processing_info.json`) and are attached under `annotations` as `time_vs_magnitude` (Finder) or `processing_info` (VS).

## Run Tests

```bash
python -m unittest tests/test_vs_parser.py
```

This VS test expects sample logs at `../test-data/parser_train_data/ELM2020/scvsmag-processing-info.log` relative to the repo root (as referenced by the test). Place the file accordingly before running.

## Key Files

- `src/eewpw_parser/schemas.py`: `FinalDoc`, `Meta`, `Detection`, `DetectionCore`, `GMObs`, `FaultVertex`, `Annotation`.
- `src/eewpw_parser/config.py`: config merge + `load_profile()` for regex profiles.
- `src/eewpw_parser/parsers/finder/`: `finder_parser.py` orchestrator, dialects in `dialects.py` (`SCFinderDialect`, `ShakeAlertFinderDialect`, `NativeFinder*`).
- `src/eewpw_parser/parsers/vs/`: `vs_parser.py` orchestrator, dialect in `dialects.py`.
- `docs/architecture.md`, `docs/parsers_finder.md`, `docs/parsers_vs.md`.

## Extending

- Add a new dialect by implementing a class with a streaming API (`parse_stream` or `feed_line` + `flush`) and mapping to `DetectionCore`/GM lists.
- Register regex patterns in `configs/profiles/<name>.json` and load via `load_profile()`.
- Mirror the Finder/VS orchestrators to merge per-file results and derive `meta.started_at/finished_at`.

## Dialects Explained

Dialects represent distinct log format variants for the same algorithm. Each dialect class encapsulates:
- Regex patterns for extracting fields (e.g., timestamps, event markers, station rows, rupture lists).
- Streaming state structures (`FinderStreamState`, `VSStreamState`, `VSEventState`) keeping partial blocks until enough lines arrive.
- Timestamp selection rules (wall-clock vs origin time fallbacks).
- Optional overrides for annotation extraction (e.g., ShakeAlert XML payload parsing in `ShakeAlertFinderDialect`).

Why dialects matter:
- Algorithms are deployed in different runtime environments (SeisComP, native local runs, ShakeAlert) producing slightly different log syntaxes.
- A single parser orchestrator (e.g., `FinderParser`) can swap dialects without changing downstream schema assembly.
- Adding a dialect isolates new regex patterns and parsing edge cases from core merging/sorting logic.

Implementing a new dialect:
1. Create a class in the appropriate `parsers/<algo>/dialects.py` file (inheriting the base dialect if desinged that way).
2. Define any additional regexes or override methods (`_parse_annotations_stream`, `_pick_detection_timestamp`, custom block parsing).
3. Ensure streaming safety: handle incomplete blocks when `finalize=False` and flush remaining state on `finalize=True` or at `flush()`.
4. Return detections and annotations shaped for the unified schema; do not reorder or dedup—global dedup occurs after merge.
5. Add a profile JSON under `configs/profiles/` if new annotation patterns are needed.

Result: The orchestrator remains unchanged; selecting the dialect just adjusts extraction specifics while preserving the output contract (`FinalDoc`).
