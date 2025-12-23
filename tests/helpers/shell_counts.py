import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parents[2]


def run_shell_marker_counts(spec: Dict[str, str], log_path: Path) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for key, cmd_tpl in spec.items():
        cmd = cmd_tpl.format(log=str(log_path))
        proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if proc.returncode != 0:
            raise AssertionError(f"Command failed ({cmd}): {proc.stderr}")
        try:
            counts[key] = int(proc.stdout.strip())
        except ValueError:
            raise AssertionError(f"Non-integer output from command ({cmd}): {proc.stdout}")
    return counts


def run_parser_and_load_output(algo: str, dialect: str, log_path: Path, out_path: Path) -> Dict:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src")
    cli = [
        sys.executable,
        str((ROOT / "src" / "eewpw_parser" / "cli.py").resolve()),
        "--algo",
        algo,
        "--dialect",
        dialect,
        "-o",
        str(out_path),
        str(log_path),
    ]
    proc = subprocess.run(cli, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        raise AssertionError(f"Parser failed: {proc.stderr}")
    with open(out_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def compute_vs_observed_counts(doc_json: Dict) -> Dict[str, int]:
    detections = doc_json.get("detections") or []
    det_count = len(detections)

    station_block_total = 0

    def count_comp(measure_list, comp: str) -> int:
        return sum(1 for obs in measure_list if ((obs.get("extra", {}).get("vs") or {}).get("component") == comp))

    gm_pga_Z = gm_pgv_Z = gm_pgd_Z = 0
    gm_pga_H = gm_pgv_H = gm_pgd_H = 0

    for det in detections:
        pga_list = det.get("gm_info", {}).get("pga_obs", []) or []
        pgv_list = det.get("gm_info", {}).get("pgv_obs", []) or []
        pgd_list = det.get("gm_info", {}).get("pgd_obs", []) or []
        unique_sncls = {obs.get("SNCL") for obs in (pga_list + pgv_list + pgd_list)}
        station_block_total += len(unique_sncls)

        gm_pga_Z += count_comp(pga_list, "Z")
        gm_pgv_Z += count_comp(pgv_list, "Z")
        gm_pgd_Z += count_comp(pgd_list, "Z")
        gm_pga_H += count_comp(pga_list, "H")
        gm_pgv_H += count_comp(pgv_list, "H")
        gm_pgd_H += count_comp(pgd_list, "H")

    return {
        "det_update_blocks": det_count,
        "station_blocks": station_block_total,
        "gm_pga_Z": gm_pga_Z,
        "gm_pgv_Z": gm_pgv_Z,
        "gm_pgd_Z": gm_pgd_Z,
        "gm_pga_H": gm_pga_H,
        "gm_pgv_H": gm_pgv_H,
        "gm_pgd_H": gm_pgd_H,
    }


def compute_finder_expected_counts(log_path: Path) -> Dict[str, int]:
    """
    Derive expected counts from scfinder.log without invoking the parser.
    """
    from eewpw_parser.parsers.finder.dialects import FinderBaseDialect

    lines = Path(log_path).read_text(encoding="utf-8", errors="ignore").splitlines()
    header_re = FinderBaseDialect.P_STATION_HEADER
    row_re = FinderBaseDialect.P_STATION_ROW
    event_re = FinderBaseDialect.P_EVENT_ID
    rupture_re = FinderBaseDialect.P_GET_RUP
    num_stations_re = FinderBaseDialect.P_GET_NUM_STATIONS

    det_updates = 0
    rupture_blocks = 0
    station_rows = 0
    num_stations_lines = 0

    pending_rows = None
    i = 0
    while i < len(lines):
        line = lines[i]

        if header_re.search(line):
            pending_rows = []
            i += 1
            continue

        matches = row_re.findall(line)
        if pending_rows is not None and matches:
            for m in matches:
                if int(m[5]) == 1:
                    pending_rows.append(m)

        if event_re.search(line):
            det_updates += 1
            j = i + 1
            while j < len(lines):
                s = lines[j]
                if header_re.search(s) or event_re.search(s):
                    break
                if rupture_re.search(s):
                    rupture_blocks += 1
                if num_stations_re.search(s):
                    num_stations_lines += 1
                j += 1

            station_rows += len(pending_rows or [])
            pending_rows = None
            i = j
            continue

        i += 1

    return {
        "det_update_blocks": det_updates,
        "rupture_blocks": rupture_blocks,
        "station_rows_included": station_rows,
        "num_stations_lines": num_stations_lines,
    }


def compute_finder_observed_counts(doc_json: Dict) -> Dict[str, int]:
    detections = doc_json.get("detections") or []
    det_count = len(detections)
    rupture_blocks = sum(1 for d in detections if d.get("fault_info"))
    station_rows = sum(len((d.get("gm_info") or {}).get("pga_obs") or []) for d in detections)
    num_stations = 0
    for det in detections:
        fd = det.get("finder_details") or {}
        sm = fd.get("solution_metrics") or {}
        if "num_stations" in sm:
            num_stations += 1
    return {
        "det_update_blocks": det_count,
        "rupture_blocks": rupture_blocks,
        "station_rows_included": station_rows,
        "num_stations_lines": num_stations,
    }
