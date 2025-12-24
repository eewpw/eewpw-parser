import subprocess
import sys
import unittest
from pathlib import Path
from dateutil import parser as dtp

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.finder.finder_parser import FinderParser  # noqa: E402


LOG_PATH = Path("/Users/savas/my-codes/eew/eewpw-project/test-data/example_logs_json_20251107/FinDer_Output/log_20140824_southnapa/log_20140824_southnapa")


def _run_cmd(cmd: str) -> str:
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        raise AssertionError(f"Command failed ({cmd}): {proc.stderr}")
    return proc.stdout.strip()


def _iso_to_epoch(ts: str) -> int:
    return int(dtp.parse(ts).timestamp())


class TestFinderLegacyShellCounts(unittest.TestCase):
    def test_counts_align_with_shell_markers(self):
        expected_det = int(_run_cmd(f"grep -c '^Timestamp =' {LOG_PATH}"))
        first_line = _run_cmd(f"grep -m1 '^Timestamp =' {LOG_PATH}")
        seed_epoch = int(first_line.split()[-1])
        expected_pga = int(_run_cmd(f"grep -c 'include = 1' {LOG_PATH}"))
        expected_rupture = int(_run_cmd(f"grep -F -c -- '-> get_rupture_list =' {LOG_PATH}"))

        parser = FinderParser({"dialect": "native_finder_legacy"})
        doc = parser.parse([str(LOG_PATH)])

        detections = doc.detections
        self.assertEqual(expected_det, len(detections))

        ts_epochs = [_iso_to_epoch(det.timestamp) for det in detections]
        self.assertEqual(seed_epoch, ts_epochs[0])
        for idx, (expected_ts, actual) in enumerate(zip(range(seed_epoch, seed_epoch + len(ts_epochs)), ts_epochs)):
            self.assertEqual(expected_ts, actual, f"timestamp step mismatch at index {idx}")

        pga_total = sum(len(d.gm_info.pga_obs) for d in detections)
        self.assertEqual(expected_pga, pga_total)

        fault_total = sum(len(d.fault_info) for d in detections)
        self.assertGreaterEqual(fault_total, expected_rupture)

        anns = doc.annotations.get("time_vs_magnitude") or []
        for ann in anns:
            self.assertTrue(
                (ann.pattern_id or "").startswith("finder/native_finder_legacy:"),
                f"annotation pattern_id not namespaced: {ann.pattern_id}",
            )


if __name__ == "__main__":
    unittest.main()
