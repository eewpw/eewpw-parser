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
