#!/usr/bin/env bash
set -euo pipefail

# === User configuration ===
FINDER_LOG=""          # path to a Finder log; leave empty to skip
FINDER_DIALECT=""      # Required: Finder dialect (e.g., scfinder)

# path to a VS log; leave empty to skip
VS_LOG="tests/test-data/scvsmag-processing-info.log"              
# Required: VS dialect (e.g., scvsmag)
VS_DIALECT="scvsmag"          
REPLAY_SPEED=1.0       # playback speed factor (<=0 treats as 1.0; 0-1 is slower-than-real-time; >1 is faster-than-real-time)
LIVE_OUTPUT_DIR="./tmp/live_output"
GRACE_SECONDS=2
CLEANUP_ON_SUCCESS=true
VERBOSE=0

# === internal helpers ===
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

if [[ ! -f "pyproject.toml" ]]; then
  echo "[harness] Run from repo root (pyproject.toml not found)" >&2
  exit 1
fi

TMP_DIR="${REPO_ROOT}/tmp"
mkdir -p "${TMP_DIR}"
mkdir -p "${LIVE_OUTPUT_DIR}"

declare -a LOGS=()

add_log() {
  local path="$1"
  local algo="$2"
  local dialect="$3"

  # Skip if no path was provided
  if [[ -z "${path}" ]]; then
    return
  fi

  # Validate existence
  if [[ ! -f "${path}" ]]; then
    echo "[harness] Missing log: ${path}" >&2
    exit 1
  fi

  # Dialect MUST be provided
  if [[ -z "${dialect}" ]]; then
    echo "[harness] Missing required dialect for ${algo} log: ${path}" >&2
    exit 1
  fi

  LOGS+=("${path}|${algo}|${dialect}")
}

add_log "${FINDER_LOG}" "finder" "${FINDER_DIALECT}"
add_log "${VS_LOG}" "vs" "${VS_DIALECT}"

if [[ ${#LOGS[@]} -eq 0 ]]; then
  echo "[harness] No logs configured; set FINDER_LOG or VS_LOG" >&2
  exit 1
fi

declare -a LIVE_PIDS=()
declare -a FAKE_TARGETS=()

cleanup() {
  local exit_code=$?
  if [[ ${#LIVE_PIDS[@]} -gt 0 ]]; then
    for pid in "${LIVE_PIDS[@]}"; do
  if kill -0 "${pid}" 2>/dev/null; then
    kill -INT "${pid}" 2>/dev/null || true
  fi
  done
    sleep 0.5
    for pid in "${LIVE_PIDS[@]}"; do
      if kill -0 "${pid}" 2>/dev/null; then
        kill -TERM "${pid}" 2>/dev/null || true
      fi
    done
  fi

  if [[ ${exit_code} -eq 0 && "${CLEANUP_ON_SUCCESS}" == "true" ]]; then
    for f in "${FAKE_TARGETS[@]}"; do
      rm -f "${f}"
    done
  fi
  exit ${exit_code}
}
trap cleanup INT TERM EXIT

# Pre-touch/truncate fake logs
for entry in "${LOGS[@]}"; do
  IFS="|" read -r src algo dialect <<< "${entry}"
  fake="${TMP_DIR}/fake_$(basename "${src}")"
  : > "${fake}"
  FAKE_TARGETS+=("${fake}")
done

# Start live clients
for entry in "${LOGS[@]}"; do
  IFS="|" read -r src algo dialect <<< "${entry}"
  fake="${TMP_DIR}/fake_$(basename "${src}")"
  cmd=(eewpw-parse-live --algo "${algo}" --logfile "${fake}" --data-root "${LIVE_OUTPUT_DIR}" --instance "${algo}@harness")
  cmd+=(--dialect "${dialect}")
  if [[ ${VERBOSE} -eq 1 ]]; then
    cmd+=(--verbose)
  fi
  cmd+=(--poll-interval 0.1)
  "${cmd[@]}" &
  LIVE_PIDS+=($!)
done

# Run replay over originals into fake logs
replay_cmd=(eewpw-replay-log --speed "${REPLAY_SPEED}")
if [[ ${VERBOSE} -eq 1 ]]; then
  replay_cmd+=(--verbose)
fi
for entry in "${LOGS[@]}"; do
  IFS="|" read -r src _algo _dialect <<< "${entry}"
  replay_cmd+=("${src}")
done

set +e
"${replay_cmd[@]}"
replay_status=$?
set -e

sleep "${GRACE_SECONDS}"

# Validate output
target_root="${LIVE_OUTPUT_DIR}/live/raw"
if [[ ! -d "${target_root}" ]]; then
  echo "[harness] Missing live output dir: ${target_root}" >&2
  exit 1
fi
shopt -s nullglob globstar
jsonl_files=("${target_root}"/**/*.jsonl)
if [[ ${#jsonl_files[@]} -eq 0 ]]; then
  echo "[harness] No JSONL outputs found in ${target_root}" >&2
  exit 1
fi

empty_count=0
for f in "${jsonl_files[@]}"; do
  if [[ ! -s "${f}" ]]; then
    empty_count=$((empty_count + 1))
  fi
done

if [[ ${empty_count} -gt 0 ]]; then
  echo "[harness] Found ${empty_count} empty JSONL files" >&2
  exit 1
fi

total_size=$(du -ch "${jsonl_files[@]}" | tail -n1 | awk '{print $1}')

echo "[harness] replay exit=${replay_status}, files=${#jsonl_files[@]}, total_size=${total_size}"
if [[ ${replay_status} -ne 0 ]]; then
  exit 1
fi

exit 0
