# eewpw-parser

Deterministic, streaming-friendly parsers for EEW (Earthquake Early Warning) algorithm logs. Current algorithms: Finder and VS. Output is a unified JSON `FinalDoc` for downstream tooling.

## Install

```bash
pip install -e .
```

Requires Python 3.9+. Runtime deps: `pydantic<2`, `python-dateutil`.

## CLI Usage

Scripts:
- `eewpw-parse`: Batch JSON or JSONL streaming.
- `eewpw-replay-log`: Pure raw-line playback to `./tmp`.
- `eewpw-parse-live`: Tail a log and write per-event JSONL.

Batch example:

```bash
eewpw-parse --algo finder --dialect scfinder --mode batch -o out.json /path/to/log1 /path/to/log2
```

Streaming JSONL example:

```bash
eewpw-parse --algo vs --dialect scvsmag --mode stream-jsonl --instance vs@node1 -o out.jsonl /path/to/scvsmag-processing-info.log
```

Notes:
- Config is merged from `configs/global.json` and `configs/<algo>.json` (e.g., `configs/finder.json`).
- Output formatting can be tuned via `output.pretty|indent|ensure_ascii` in config.
- Detections are sorted by `timestamp`; per-file extras are emitted under `meta.extras["files"]`.

## Replay Log CLI

Pure playback of original logs. Reads raw lines from one or more input logs and writes them to fake files under `./tmp` as `fake_<basename>`. No parsing. No JSON/JSONL. No schemas or live engine.

Example commands:

```bash
eewpw-replay-log --speed 10.0 /path/to/A.log /path/to/B.log
# Writes tmp/fake_A.log and tmp/fake_B.log. Truncates before playback.
```

```bash
eewpw-replay-log --speed 10.0 --repeat 3 /path/to/A.log /path/to/B.log
# Same as the example above, but repeats the same replay three times.
```

```bash
eewpw-replay-log --file-list ./logs.txt --speed 5
# logs.txt contains one path per line; ignores empty lines and comments beginning with #.
```

```bash
cat paths.txt | eewpw-replay-log --speed 2
# If no positional args and stdin is not a TTY, reads paths from stdin.
```

Notes:
- In original mode, the sleep baseline is the earliest timestamp across all inputs. In realtime mode (rebased), the earliest original timestamp is mapped to current UTC time at replay start.
- Sleep = delta_seconds / speed. If speed <= 0, treat as 1; if speed < 0.001, clamp to 0.001.
- If a line has no timestamp, reuse the previous line’s timestamp; if the first line has no timestamp, write without sleeping.
- Always writes to `./tmp`; never writes next to the input logs.

### Time Mode

The replay CLI supports `--time-mode {original,realtime}` (default `original`).

- `original`: use timestamps as found in logs; sleep deltas relative to earliest original timestamp.
- `realtime`: rebase earliest timestamp to now (UTC); preserve relative intervals and repeat cycle offsets.
- Rebasing formula: `t_new = T0_sim + (t_orig - T0_orig) + cycle_offset` where `T0_sim = datetime.now(UTC)` at start.
- With `--repeat N`, each cycle is offset by the file’s span (latest - earliest) after rebasing.

```bash
eewpw-replay-log --time-mode original --speed 5 path/to/scfinder.log
eewpw-replay-log --time-mode realtime --speed 5 path/to/scfinder.log
eewpw-replay-log --time-mode realtime --repeat 2 path/to/vs.log
```

Use realtime mode to simulate historical logs as if they were emitted today so downstream live daily merging triggers.

## Live CLI

Tail a live log and stream per-event JSONL files.

```bash
eewpw-parse-live --algo vs --dialect scvsmag --logfile /path/to/log --output-dir ./out --instance vs@node1
```

Notes:
- Writes per-event JSONL files to `--output-dir`.
- Polling interval can be tuned via `--poll-interval` (default 0.1s).

## Streaming Design

- Finder: `FinderParser` orchestrates dialects in `parsers/finder/dialects.py` using `parse_stream(lines, state, finalize)`; keeps a `FinderStreamState` for partial blocks and station tables.
- VS: `VSDialect` processes lines incrementally via `feed_line(...)` + `flush(...)` and tracks a `VSEventState` per event between “Start/End logging for event”.
- Annotations use regex profiles (e.g., `configs/profiles/vs_time_vs_mag.json`) and are attached under `annotations` as `time_vs_magnitude` (both for Finder and VS).

## Run Tests

Run all tests:
```bash
python -m unittest
```

Or a specific test:
```bash
python -m unittest tests/test_vs_parser.py
```

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
