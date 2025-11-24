# Finder Parser

## Overview

Finder parsing lives in `src/eewpw_parser/parsers/finder/` and is orchestrated by `FinderParser` (`finder_parser.py`). Dialect classes in `dialects.py` hold the regexes and extraction rules for different log styles:
- `SCFinderDialect`: standard scfinder logs (prefix timestamp per line).
- `ShakeAlertFinderDialect`: scfinder used inside ShakeAlert, often with embedded XML `event_message` payloads.
- `NativeFinderDialect`: direct Finder output with per-line timestamps.
- `NativeFinderLegacyDialect`: legacy Finder without per-line wall-clock stamps; falls back to epoch timestamps inside detection blocks.

`FinderParser.parse(inputs)` collects detections/annotations from each file, merges them, sorts by detection timestamp, derives meta timing from per-file extras, and returns a `FinalDoc`.

`FinderParser.parse(inputs, sink=None)` also supports streaming: when a sink is provided, detections/annotations are emitted to the sink as they are parsed; meta is finalized at the end and no `FinalDoc` is returned.

## Detection extraction

- Single-pass parsing: `FinderBaseDialect.parse_file` streams the file in batches, so detection and annotation extraction happen without a full-file read.
- Detection blocks are anchored by `event_id` lines. The parser scans forward through the block to capture:
  - `get_mag`, `get_epicenter_lat/lon`, `get_depth`, `get_likelihood`, `get_origin_time` (epoch), rupture vertices (`get_rupture_list` + continuation lines).
  - The first prefix timestamp (`P_PREFIX_TS`) in the block is used as the emission time when available.
  - Stations that exceeded thresholds are parsed from station tables (`P_STATION_HEADER` + `P_STATION_ROW`) and attached as `pga_obs`.
- Versioning is tracked per `event_id` to reflect successive updates in the log stream.
- Dialects can override `_pick_detection_timestamp` to choose wall-clock vs origin/epoch timestamps (used by the legacy native dialect).

## Annotations

- A profile JSON (`configs/profiles/finder_time_vs_mag.json` for scfinder) defines regex patterns for notable lines. `_parse_annotations` walks the log, normalizes timestamps from the prefix, and records matches.
- File-level timing comes from the first/last timestamped lines; playback time is captured when the START_PLAYBACK pattern is present.

## Real-time considerations

- The parser can operate incrementally over line streams: state tracks the current detection block and pending station list so detections can be emitted when a new `event_id` is encountered or on flush.
- Timestamp selection always prefers the wall-clock prefix seen so far; otherwise it falls back to origin/epoch values so detections can be emitted even before the block is complete.
- Station blocks are consumed once and applied to the next detection emission to avoid buffering entire files.

## Rolling Buffer (Finder)

- `FinderStreamState` maintains a bounded `recent_lines` deque (max 2000 entries) with tuples `(line_number, line_text)` and an `absolute_line_counter`.
- During streaming (`parse_stream`), each complete line increments the counter and is pushed into `recent_lines`; incomplete trailing lines are held until finalized.
- The buffer is for diagnostics/forward-looking features; parsing semantics are unchanged and the buffer is not yet used for lookbacks.

## Limitations / Improvements

- Regexes are tuned to current samples and may miss variant Finder wording.
- Station tables currently assume contiguous rows after the header; interleaved formats would need a smarter state machine.
- `_parse_detections_stream` still relies on a pending-station list that maps a station table to the next detection; if logs interleave multiple station tables before an event block, we may attach the wrong list.
- Real-time mode now shares the same code path as offline parsing, but more tail-based testing against live Finder output is recommended (especially for multi-file merges).
