# -*- coding: utf-8 -*-
import json
import subprocess
from pathlib import Path
from typing import Optional
from dateutil import parser as dtp

from eewpw_parser.parsers.finder.finder_parser import FinderParser


def _run_cmd(cmd: str) -> str:
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        raise AssertionError(f"Command failed ({cmd}): {proc.stderr}")
    return proc.stdout.strip()


def _iso_to_epoch(ts: str) -> int:
    return int(dtp.parse(ts).timestamp())


def verify_native_finder_legacy(
    raw_log_path: str,
    output_json_path: str,
    prev_json_path: Optional[str] = None,
) -> None:
    log_path = Path(raw_log_path)
    out_path = Path(output_json_path)

    expected_det = int(_run_cmd(f"grep -c '^Timestamp =' {log_path}"))
    expected_seed = _run_cmd(f"grep -m1 '^Timestamp =' {log_path}")
    try:
        seed_epoch = int(expected_seed.split()[-1])
    except ValueError:
        raise AssertionError(f"Unable to parse seed epoch from line: {expected_seed}")
    expected_pga = int(_run_cmd(f"grep -c 'include = 1' {log_path}"))
    expected_rupture = int(_run_cmd(f"grep -F -c -- '-> get_rupture_list =' {log_path}"))

    parser = FinderParser({"dialect": "native_finder_legacy"})
    doc = parser.parse([str(log_path)])

    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(doc.model_dump(), fh, ensure_ascii=False)

    detections = doc.detections
    if len(detections) != expected_det:
        raise AssertionError(f"detection count mismatch: expected {expected_det} got {len(detections)}")

    ts_epochs = [_iso_to_epoch(det.timestamp) for det in detections]
    if ts_epochs[0] != seed_epoch:
        raise AssertionError(f"first detection timestamp mismatch: expected {seed_epoch} got {ts_epochs[0]}")
    for idx, (expected_ts, actual) in enumerate(zip(range(seed_epoch, seed_epoch + len(ts_epochs)), ts_epochs)):
        if expected_ts != actual:
            raise AssertionError(f"timestamp step mismatch at index {idx}: expected {expected_ts} got {actual}")

    pga_total = sum(len(d.gm_info.pga_obs) for d in detections)
    if pga_total != expected_pga:
        raise AssertionError(f"pga_obs count mismatch: expected {expected_pga} got {pga_total}")

    fault_total = sum(len(d.fault_info) for d in detections)
    if fault_total < expected_rupture:
        raise AssertionError(f"fault vertex count too low: expected >= {expected_rupture} got {fault_total}")

    anns = doc.annotations.get("time_vs_magnitude") or []
    for ann in anns:
        pid = ann.pattern_id or ""
        if not pid.startswith("finder/native_finder_legacy:"):
            raise AssertionError(f"annotation pattern_id not namespaced: {pid}")

    if prev_json_path:
        prev = json.loads(Path(prev_json_path).read_text(encoding="utf-8"))
        prev_det = len(prev.get("detections") or [])
        if prev_det != len(detections):
            raise AssertionError(f"previous JSON detection count mismatch: expected {prev_det} got {len(detections)}")
