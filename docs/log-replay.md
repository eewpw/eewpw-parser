# Log Replay (`eewpw-replay-log`)

`eewpw-replay-log` replays existing log files as if they were being produced live.  
It rewrites timestamps and writes the replayed stream into temporary files under `./tmp`.  

This tool is intended for testing, demonstrations, and simulating real‑time log streams.  
It does **not** parse logs into EEWPW JSON schema objects; it only replays raw log lines.

## Command synopsis

```bash
eewpw-replay-log [--speed N] [--time-mode {original,realtime}] [--file-list PATH] [--repeat N] [-v] [inputs...]
```

| Flag / argument | Required | Default | Meaning |
|---|---|---|---|
| `inputs` (positional) | no* | none | One or more input log files to replay. |
| `--speed` | no | `1.0` | Replay speed factor used for sleeps between lines (actual behavior for `<=0` is documented below). |
| `--time-mode` | no | `original` | Timestamp handling mode: `original` or `realtime`. |
| `--file-list` | no | none | Path to a file listing log paths (one per line, `#` comments allowed). |
| `--repeat` | no | `1` | Repeat each input file N times in its own fake output. |
| `-v`, `--verbose` | no | `false` | Print additional completion/progress messages. |

\* At least one input source must be provided: positional `inputs`, or `--file-list`, or stdin path list (when stdin is not a TTY and no positional/file-list is provided).

## Basic usage

Typical workflow:

1. Choose one or more existing log files.  
2. Run `eewpw-replay-log` with those files.  
3. The tool writes replayed logs to `./tmp/fake_<filename>.log`.  
4. Other tools can then read these fake logs as if they were produced in real time.

## Input files

Input paths are collected from:
- positional `inputs`
- `--file-list`
- stdin path list, only when both positional inputs and `--file-list` are absent and stdin is not a TTY

Validation/collection rules:
- each collected path must be a regular file (`is_file()`), otherwise the command exits with code 1
- duplicate paths are deduplicated by exact path string
- empty total input set exits with code 1

## Time behaviour

Modes:
- `original`: baseline is the global earliest original timestamp across all inputs
- `realtime`: baseline is current UTC (`datetime.now(timezone.utc)`), with original relative offsets preserved

Timestamp rewriting behavior:
- when a timestamp pattern is recognized, rewritten output preserves original structural formatting:
  - date separator (`-` vs `/`)
  - date-time separator (` `, `T`, or `,`)
  - fractional separator (`.` vs `:`)
  - fractional precision length
- lines without a recognized timestamp are written unchanged

## Important options

`--speed` option behavior:
- `speed <= 0` is clamped to `1.0` (sleeping is not disabled)
- `0 < speed < 0.001` is clamped to `0.001`

`--repeat` option behavior:
- each source file is replayed repeatedly into its own `fake_<basename>.log`
- cycle spacing per file:
  - `latest_ts - earliest_ts` when at least two timestamps exist
  - `60s` when exactly one timestamp exists
  - no cycle offset when no timestamps exist
- `--repeat <= 0` behaves like `1` (`max(1, repeat)`)

## Output files

- For each input file, output path is `./tmp/fake_<basename>.log`.
- Target fake file is created/truncated before replay of that source file.
- Replayed lines are written to fake files; stdout is used for progress/verbose messages only.
- Basename collision caveat: different source paths with the same basename map to the same fake output filename.

## Config independence

- Replay does not load parser configs (`global.json`) or parser profiles.
- Replay does not support `--config-root`; passing it is rejected as an unrecognized argument.

## Limitations

Current behavioral limitations:
- untimestamped-line timing edge behavior is not covered by tests

## Appendix — Technical audit

The following notes describe implementation details and observations from internal audits. They are not required to use the tool but are kept here for completeness.

### Timestamp parse normalization:

- naive parsed timestamps are treated as UTC
- timezone-aware parsed timestamps are converted to UTC

### Ordering model

Timestamp extraction precedence per line:
1. prefix timestamp
2. inline timestamp
3. fallback parse of the full line

Multi-file behavior:
- the command computes one earliest timestamp per file
- files with timestamps are sorted by that earliest file timestamp
- files with no timestamps are appended at the end, preserving original relative order
- replay is sequential per file; there is no global line-level interleaving across files

### Current coverage gaps (per audit report):

- no tests for `--file-list`
- no tests for stdin path input mode
- no tests for multi-file ordering behavior
- no tests for basename collision behavior
- no tests for sleep timing behavior
