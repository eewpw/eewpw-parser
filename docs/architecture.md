# Architecture

This repository provides deterministic parsers for EEWPW algorithm logs. Parsers share a small set of utilities and schemas under `src/eewpw_parser` and expose a CLI via `eewpw_parser.cli`.

- `schemas.py` contains the Pydantic models used for detections (`Detection`, `DetectionCore`, `FaultVertex`, `GMObs`), annotations, and the `Meta`/`FinalDoc` envelope.
- `config.py` loads global and algorithm-specific JSON config plus reusable profile snippets (e.g., regex-driven annotation profiles).
- `utils.py` provides timestamp normalization helpers (`to_iso_utc_z`, `epoch_to_iso_z`) used by all parsers.
- `parsers/` holds per-algorithm implementations. Finder and VS are implemented; additional algorithms can plug in using the same patterns.
- `cli.py` is the entrypoint (`eewpw-parse`) that wires config, selects a parser, and writes a JSON `FinalDoc`.

## Parser shape and responsibilities

- Parsers produce a `FinalDoc` with:
  - `meta`: algo id, dialect, file-level timing, stats, extras.
  - `annotations`: collected regex matches for log markers.
  - `detections`: ordered list of `Detection` items with core, fault, and GM info.
- Each algorithm has dialect-specific logic encapsulated in dedicated classes (e.g., `SCFinderDialect`, `ShakeAlertFinderDialect`). Dialects keep regexes and timestamp rules together.
- Real-time orientation: parsing logic is organized so it can operate incrementally on line streams (tailing live logs) rather than assuming the whole file. State objects buffer partial blocks until they can be emitted, and timestamp fallbacks avoid end-of-file assumptions.

## Real-time / tailing concept

- Lines are ingested in order; parsers maintain per-block state (e.g., “current detection block” for Finder, `VSEventState` for VS) and emit detections as soon as enough fields arrive.
- Annotation detection runs per line using configured regex profiles; timestamps are derived from log prefixes when available.
- File-level `started_at` / `finished_at` come from the first/last seen timestamps; in a live tail they can be updated as new lines arrive.
- Extras for playback or per-file stats are accumulated incrementally so a streaming orchestrator can refresh `meta` without reparsing the full file.

## Output sinks

- A sink abstraction is scaffolded in `src/eewpw_parser/sinks.py`:
  - `FinalDocSink` is intended for batch/offline runs that assemble a single `FinalDoc`.
  - `JsonlStreamSink` is intended for streaming/tailing scenarios to emit JSONL records incrementally.
- Future parser orchestrators will push detections/annotations/meta into these sinks to decouple parsing from output handling.
- CLI supports `--mode batch` (default) to emit a single JSON file or `--mode stream-jsonl` to emit JSONL lines (`record_type`, `algo`, `dialect`, `instance`, `payload`) via `JsonlStreamSink`. An optional `--instance` sets the instance id (default `<algo>@unknown`).
- Replay CLI (`eewpw-parse-replay`) replays logs to JSONL using sinks; timing is gated by a speed factor and meta.extras carries a replay note.

### Sink details and JSONL envelope

- `BaseSink` protocol defines `start_run`, `emit_detection`, `emit_annotation`, and `finalize`.
- `FinalDocSink` collects detections/annotations in memory, deduplicates and sorts detections by timestamp, and emits a `FinalDoc` on finalize (batch behaviour).
- `JsonlStreamSink` writes one JSON line per record with envelope keys:
  - `record_type`: one of `detection`, `annotation`, `meta`.
  - `algo`, `dialect`, `instance`: identify the source.
  - `payload`: `model_dump()` of the Pydantic model (Detection/Annotation/Meta).
- Streaming outputs always end with exactly one `meta` record.
