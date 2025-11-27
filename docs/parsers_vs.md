# VS Parser (design)

Target log style: `scvsmag-processing-info.log` under `test-data/parser_train_data/ELM2020/`. Each line has a prefix timestamp `[processing/info/VsMagnitude]` followed by structured key/value phrases.

## Expected patterns

- Session markers:
  - `scvsmag module started at <ISO>` (initial startup)
  - `Start logging for event: <event_id>`
  - `End logging for event: <event_id>`
  - `update number: <n>` (per-event version counter)
- Per-station payloads (repeated between Start/End):
  - `Sensor: <net.sta>.<loc>.<chan>; Wavetype: <P|S>-wave; Soil class: <...>; Magnitude: <float|nan>`
  - Next lines give `station lat`, `station lon`, `epicentral distance`
  - PGA/PGV/PGD for Z/H components
- Aggregate summary near the end of a block:
  - `VS-mag: <float>; median single-station-mag: <float>; lat: <float>; lon: <float>; depth : <float> km`
  - `creation time: <ISO>; origin time: <ISO>; ...`
  - counts (`# picked stations`, `# envelope streams`, distance threshold info)
  - `likelihood: <float>`

## Mapping to shared schema

- `Detection.timestamp`: wall-clock from the prefix of the summary/likelihood line for the block; falls back to `creation time`.
- `event_id`: taken from the surrounding Start/End markers.
- `version`: derived from `update number`.
- `core_info`:
  - `id`: event id
  - `mag`: VS-mag value
  - `lat`/`lon`/`depth`: from the VS-mag summary
  - `orig_time`: from `origin time`
  - `likelihood`: from `likelihood:` line
- `fault_info`: not present in VS logs; emit empty list.
- `gm_info.pga_obs`: built from per-station Sensor blocks with PGA(H) and PGA(Z) magnitudes; station coordinates included.

## Dialect structure

- Single dialect for now: `ELM2020` (SeisComP `scvsmag` info logs).
- Parser keeps a state machine per event:
  - On `Start logging for event`, flush any current block and begin a new one.
  - Consume Sensor blocks and summary lines until `End logging for event` (or next Start), then emit a detection.
  - In streaming mode, if the tail ends mid-block, `flush()` emits what is available using the latest timestamps/magnitudes seen.

## Real-time handling

- Lines are processed as they arrive; partial blocks are kept in memory.
- Block closes on `End logging for event` or when a new `Start logging` appears for the same event (implicit flush).
- Timestamps are updated per line so `started_at`/`finished_at` reflect the stream window without re-reading the file.
- Malformed numeric fields are skipped, but the detection is still emitted with available values.

## Error handling

- Non-parsable lines are ignored.
- Missing VS-mag or origin time uses safest defaults: mag=0.0, depth=0.0, `orig_time` from the latest prefix timestamp, likelihood None.
- `nan` station magnitudes and sentinel `-1.00e+00` values are dropped from GM observations.

## Implementation (current)

- Dialect class: `VSDialect` in `src/eewpw_parser/parsers/vs/dialects.py`.
- Streaming state: `VSStreamState` keeps file-level timestamps, version counters, and the active `VSEventState`.
- Event parsing logic:
  - `feed_line` normalizes the prefix timestamp, records annotations using `configs/profiles/vs_time_vs_mag.json`, and updates file start/end times.
  - `Start logging for event` flushes any in-flight block and creates a new `VSEventState`; `End logging` finalizes it into a `Detection`.
  - Sensor blocks are accumulated into `VSEventState.stations` with PGA(H)/PGA(Z) where present; `nan` and `-1` sentinels are ignored.
  - VS-mag, creation/origin times, and likelihood are captured from their dedicated lines; timestamps are normalized via `to_iso_utc_z`.
- `VSParser.parse` mirrors Finder’s orchestrator: merges per-file detections/annotations, sorts by timestamp, derives meta timings, and returns a `FinalDoc`.
- `VSParser.parse(inputs, sink=None)` supports streaming sinks: with a sink, detections/annotations are emitted as they are parsed and meta is finalized to the sink; batch path (sink=None) returns a `FinalDoc`.

## Rolling Buffer (VS)

- `VSStreamState` keeps a bounded `recent_lines` deque (max 2000 entries) with `(line_number, line_text)` and an `absolute_line_counter`.
- Each call to `feed_line` records the incoming line into this buffer before parsing; buffers are not yet used for lookbacks.
- The buffer is intended for diagnostics and future real-time features; detection/annotation semantics remain unchanged.
