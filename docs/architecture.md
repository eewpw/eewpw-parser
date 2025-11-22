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
