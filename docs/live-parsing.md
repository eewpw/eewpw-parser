# Live Parsing (`eewpw-parse-live`)

`eewpw-parse-live` tails one logfile and emits live raw JSONL records under a data root.  
It is a dedicated live-tail command, not the same as offline parsing with `eewpw-parse`.

## Command synopsis

```bash
eewpw-parse-live --algo <algo> --dialect <dialect> --logfile <path> [options]
```

| Flag | Required | Default | Meaning |
|---|---|---|---|
| `--algo` | yes | none | Algorithm selector. CLI accepts: `finder`, `vs`, `plum`, `epic`, `gfast`, `eqinfo`. |
| `--dialect` | yes | none | Dialect string (runtime validation depends on algorithm; see below). |
| `--logfile` | yes | none | Single logfile path to tail. |
| `--instance` | no | resolved at runtime to `<algo>@unknown` | Instance label written into output envelopes. |
| `--output-dir` | no | none | Deprecated fallback output root (treated as `data_root` if `--data-root` is not set). |
| `--data-root` | no | resolved by precedence | Preferred output root for live raw files; overrides `--output-dir` if both are provided. |
| `--config-root` | no | none | Optional config root override for loading `global.json`, `annotations.json`, and legacy fallback profiles. |
| `--verbose` | no | `false` | Enables verbose runtime output. |
| `--poll-interval` | no | `0.1` | Tail polling interval in seconds. |

## Basic usage

Typical workflow:

1. Choose a logfile produced by the algorithm you want to monitor.
2. Start `eewpw-parse-live` pointing to that logfile.
3. As new lines are appended to the logfile, the tool emits JSONL records.
4. The records are written under `data_root/live/raw/` and can be consumed by other tools or dashboards.

## Supported algorithms

Currently supported in live mode:

- `finder`
- `vs`
- `plum`
- `epic`

The CLI accepts additional algorithm names (`gfast`, `eqinfo`), but these are not supported in live mode and will exit with an error.

## Logfile tailing

- Exactly one logfile is monitored (`--logfile`).
- The command follows the file as new lines are appended.
- Only new log entries are processed; existing content in the file is skipped when the command starts.
- When no new lines appear, the command waits for a short interval (`--poll-interval`) before checking again.

## Output layout and record format

Live output is JSONL-only (not batch JSON).
`eewpw-parse-live` does not use or accept `--mode`.

Path layout:

```text
data_root/live/raw/<algo>/<YYYY-MM-DD>_<algo>.jsonl
```

Daily behavior:
- Records are appended to per-algorithm daily files.
- The writer rotates by record date (`YYYY-MM-DD`) as timestamps cross day boundaries.

## Config and path resolution

Data root precedence:
1. `--data-root` (if set)
2. `--output-dir` (deprecated fallback, only when `--data-root` is not set)
3. `EEWPW_DATA_ROOT`
4. `global.json` -> `live.data_root`
5. `./data` (current working directory)

Config root precedence:
1. `--config-root` (if set)
2. `EEWPW_PARSER_CONFIG_ROOT` (if valid directory)
3. packaged defaults under `src/eewpw_parser/configs/`

Annotation customization:
- Put custom patterns in `annotations.json` in your config root.
- Pass that folder with `--config-root` (or set `EEWPW_PARSER_CONFIG_ROOT`).
- `profiles/*.json` is fallback compatibility config and is deprecated/planned for removal.

## Limitations

- Live runtime currently does not support `gfast` or `eqinfo`.

## Examples

Minimal VS live example:

```bash
eewpw-parse-live \
  --algo vs \
  --dialect scvsmag \
  --logfile /path/to/scvsmag-processing-info.log \
  --data-root ./data
```

Canonical Finder example:

```bash
eewpw-parse-live \
  --algo finder \
  --dialect scfinder \
  --logfile /path/to/finder.log \
  --data-root ./data
```

EPIC `shakealert` example:

```bash
eewpw-parse-live \
  --algo epic \
  --dialect shakealert \
  --logfile /path/to/epic.log \
  --data-root ./data
```

Example with explicit `--instance`:

```bash
eewpw-parse-live \
  --algo finder \
  --dialect scfinder \
  --instance finder@node1 \
  --logfile /path/to/finder.log \
  --data-root ./data
```

## Appendix — Technical audit

The following notes describe implementation details and observations from internal development and testing. They are not required to operate the tool but are kept here for completeness.

### Supported algorithms and dialects (technical details)

Dialect behavior in live mode:
- Finder validates canonical dialects and aliases at parser runtime.
- EPIC requires dialect `shakealert`.
- VS and PLUM have weaker live validation: user-supplied `--dialect` is not strongly validated in this path; parser behavior remains fixed to `VSDialect` / `PlumParser`, and the supplied dialect string is propagated into output metadata.

### How tailing works (technical details)

- Exactly one logfile is tailed (`--logfile`).
- Tailing starts at EOF (`seek_end=True`), so existing file content is skipped.
- Only newly appended lines are followed (`follow=True`).
- When no new line exists, the loop sleeps for `--poll-interval` seconds and polls again.
- On shutdown (including `Ctrl+C`), the engine flushes and writes a final `meta` record, then closes output.

### Output layout and record format (technical details)

Envelope fields (high level):
- `record_type` (`detection`, `annotation`, `meta`)
- `algo`, `dialect`, `instance`
- `event_id`
- `timestamp`
- `payload`
- optional `profile` (annotation records)

Notes:
- Annotation records are emitted when parser annotations are emitted.
- Annotation profile key is `time_vs_magnitude`.
- Annotation `event_id` uses the most recent emitted detection id, or `""` if none has been emitted yet.
- A final `meta` record is written on shutdown.

### Limitations and test status (technical details)

- Live CLI tests currently cover VS startup and unknown-`--algo` rejection; they do not provide broad CLI coverage for Finder/PLUM/EPIC live startup paths.
- PLUM and EPIC live support has limited direct CLI test coverage.
- PLUM caveat: PLUM stream annotations can carry an empty timestamp (`""`), while live engine annotation emission parses annotation timestamps before writing; if such annotations are emitted in live mode, this can fail. This path is not covered by live tests.
