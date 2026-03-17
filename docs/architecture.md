# Architecture

This repository provides deterministic parsers for EEWPW algorithm logs. Parsers share a small set of utilities and schemas under `src/eewpw_parser` and expose a CLI via `eewpw_parser.cli`.

- `schemas.py` contains the Pydantic models used for detections (`Detection`, `DetectionCore`, `FaultVertex`, `GMObs`), annotations, and the `Meta`/`FinalDoc` envelope.
- `config.py` loads `global.json` plus reusable profile snippets (`profiles/*.json`) through `config_loader.py`. Runtime config root precedence is: CLI `--config-root` > `EEWPW_PARSER_CONFIG_ROOT` > packaged defaults under `src/eewpw_parser/configs/` (no repo-root fallback). Repo-root `./example-configs` is example-only.
- `utils.py` provides timestamp normalization helpers (`to_iso_utc_z`, `epoch_to_iso_z`) used by all parsers.
- `parsers/` holds per-algorithm implementations. Implemented families are `finder`, `vs`, `plum`, `epic`, `gfast`, and `eqinfo`; additional algorithms can plug in using the same patterns.
- `cli.py` is the entrypoint (`eewpw-parse`) that wires config, selects a parser, and writes a JSON `FinalDoc`.

## Parser shape and responsibilities

- Parsers produce a `FinalDoc` with:
  - `meta`: algo id, dialect, file-level timing, stats, extra.
  - `annotations`: collected regex matches for log markers.
  - `detections`: ordered list of `Detection` items with core, fault, and GM info.
- Each algorithm has dialect-specific logic encapsulated in dedicated classes (e.g., `SCFinderDialect`, `ShakeAlertFinderDialect`). Dialects keep regexes and timestamp rules together.
- Real-time orientation: parsing logic is organized so it can operate incrementally on line streams (tailing live logs) rather than assuming the whole file. State objects buffer partial blocks until they can be emitted, and timestamp fallbacks avoid end-of-file assumptions. Streaming states also keep a bounded `recent_lines` buffer with line numbers (capacity 2000) for diagnostics/future lookbacks without changing parsing semantics.

## Real-time / tailing concept

- Lines are ingested in order; parsers maintain per-block state (e.g., “current detection block” for Finder, `VSEventState` for VS) and emit detections as soon as enough fields arrive.
- Annotation detection runs per line using configured regex profiles; timestamps are derived from log prefixes when available. `load_profile()` strips `patterns.timestamp_regex`, so parser runtime consumes only active annotation pattern entries.
- File-level `started_at` / `finished_at` come from the first/last seen timestamps; in a live tail they can be updated as new lines arrive.
- Extra payloads for playback or per-file stats are accumulated incrementally so a streaming orchestrator can refresh `meta` without reparsing the full file.

## Output sinks

- A sink abstraction is scaffolded in `src/eewpw_parser/sinks.py`:
  - `FinalDocSink` is intended for batch/offline runs that assemble a single `FinalDoc`.
- `JsonlStreamSink` is intended for streaming/tailing scenarios to emit JSONL records incrementally.
- Future parser orchestrators will push detections/annotations/meta into these sinks to decouple parsing from output handling.
- CLI supports `--mode batch` (default) to emit a single JSON file or `--mode stream-jsonl` to emit JSONL lines (`record_type`, `algo`, `dialect`, `instance`, `payload`) via `JsonlStreamSink`. An optional `--instance` sets the instance id (default `<algo>@unknown`).
- Replay CLI (`eewpw-replay-log`) is a pure playback helper that copies raw lines into `./tmp/fake_<basename>.log` with optional timing sleeps; it does not invoke parsers, sinks, or schemas. It supports `--time-mode {original,realtime}`; in realtime mode the earliest original timestamp is mapped to current UTC (`T0_sim`) and each line's new timestamp is `T0_sim + (t_orig - T0_orig) + cycle_offset` preserving relative intervals and repeat cycles.
- Replay is config-free for parser runtime purposes: it does not read `global.json`/profiles and has no `--config-root` handling.

### Live raw storage

- Live mode uses `DailyAlgoWriter` to append envelopes into `data_root/live/raw/<algo>/<YYYY-MM-DD>_<algo>.jsonl` instead of per-event files.
- Envelopes carry `record_type`, `algo`, `dialect`, `instance`, `event_id`, `timestamp`, `payload`, and optional `profile` for annotations; a `meta` record is appended during shutdown.
- `live_cli` resolves `data_root` from `--data-root` (preferred), falls back to `--output-dir`, else uses config/env via `get_data_root`.
- Retention is handled externally via `scripts/cleanup_live_raw.py` (default keep today + yesterday).

### Sink details and JSONL envelope

- `BaseSink` protocol defines `start_run`, `emit_detection`, `emit_annotation`, and `finalize`.
- `FinalDocSink` collects detections/annotations in memory, deduplicates and sorts detections by timestamp, and emits a `FinalDoc` on finalize (batch behaviour).
- `JsonlStreamSink` writes one JSON line per record with envelope keys:
  - `record_type`: one of `detection`, `annotation`, `meta`.
  - `algo`, `dialect`, `instance`: identify the source.
  - `payload`: `model_dump()` of the Pydantic model (Detection/Annotation/Meta).
- Streaming outputs always end with exactly one `meta` record.

## Live Follow Mode

- Live follow is implemented via `eewpw-parse-live` using `TailLineSource` + `LiveEngine`.
- The parser pipeline remains streaming-oriented: lines are tailed, parsed incrementally, and written by `DailyAlgoWriter` as JSONL envelopes with a trailing `meta` record.
- Live mode currently supports `finder`, `vs`, `plum`, and `epic`; `gfast` and `eqinfo` are rejected at runtime.
- Example shape:
  ```python
  source = TailLineSource("/var/log/finder.log", poll_interval=0.5, seek_end=True, follow=True)
  parser = FinderParser({"algo": "finder", "dialect": "scfinder"})
  engine = LiveEngine(
      source=source,
      parser=parser,
      data_root=Path("/tmp/data"),
      algo="finder",
      dialect="scfinder",
      instance="finder@node1",
  )
  engine.run_forever()
  ```
