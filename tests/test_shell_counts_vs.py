import tempfile
import unittest
from pathlib import Path

from tests.helpers.shell_counts import (
    run_shell_marker_counts,
    run_parser_and_load_output,
    compute_vs_observed_counts,
)

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_LOG = ROOT / "example-data" / "Elm2020" / "scvsmag-processing-info.log"


class TestVSShellDerivedCounts(unittest.TestCase):
    def test_shell_counts_vs(self):
        spec = {
            "det_update_blocks": "grep -F -c 'Start logging for event:' {log}",
            "station_blocks": "grep -F -c 'Sensor:' {log}",
            "gm_lines_Z": "grep -F -c 'PGA(Z):' {log}",
            "gm_lines_H": "grep -F -c 'PGA(H):' {log}",
        }

        with tempfile.TemporaryDirectory() as td:
            out_path = Path(td) / "out.json"
            expected = run_shell_marker_counts(spec, SAMPLE_LOG)
            doc_json = run_parser_and_load_output("vs", "scvsmag", SAMPLE_LOG, out_path)
            observed = compute_vs_observed_counts(doc_json)

        self.assertEqual(
            observed["det_update_blocks"],
            expected["det_update_blocks"],
            f"det_update_blocks mismatch: expected {expected['det_update_blocks']} observed {observed['det_update_blocks']}",
        )
        self.assertEqual(
            observed["station_blocks"],
            expected["station_blocks"],
            f"station_blocks mismatch: expected {expected['station_blocks']} observed {observed['station_blocks']}",
        )

        for k in ["gm_pga_Z", "gm_pgv_Z", "gm_pgd_Z"]:
            self.assertEqual(
                observed[k],
                expected["gm_lines_Z"],
                f"{k} mismatch: expected {expected['gm_lines_Z']} observed {observed[k]}",
            )
        for k in ["gm_pga_H", "gm_pgv_H", "gm_pgd_H"]:
            self.assertEqual(
                observed[k],
                expected["gm_lines_H"],
                f"{k} mismatch: expected {expected['gm_lines_H']} observed {observed[k]}",
            )


if __name__ == "__main__":
    unittest.main()
