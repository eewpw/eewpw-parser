import tempfile
import unittest
from pathlib import Path

from tests.helpers.shell_counts import (
    compute_finder_expected_counts,
    compute_finder_observed_counts,
    run_parser_and_load_output,
)

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_LOG = ROOT / "example-data" / "Elm2020" / "scfinder.log"


class TestFinderShellDerivedCounts(unittest.TestCase):
    def test_shell_counts_finder(self):
        with tempfile.TemporaryDirectory() as td:
            out_path = Path(td) / "out.json"
            expected = compute_finder_expected_counts(SAMPLE_LOG)
            doc_json = run_parser_and_load_output("finder", "scfinder", SAMPLE_LOG, out_path)
            observed = compute_finder_observed_counts(doc_json)

        self.assertEqual(
            observed["det_update_blocks"],
            expected["det_update_blocks"],
            f"det_update_blocks mismatch: expected {expected['det_update_blocks']} observed {observed['det_update_blocks']}",
        )
        self.assertEqual(
            observed["rupture_blocks"],
            expected["rupture_blocks"],
            f"rupture_blocks mismatch: expected {expected['rupture_blocks']} observed {observed['rupture_blocks']}",
        )
        self.assertEqual(
            observed["station_rows_included"],
            expected["station_rows_included"],
            f"station_rows_included mismatch: expected {expected['station_rows_included']} observed {observed['station_rows_included']}",
        )
        self.assertEqual(
            observed["num_stations_lines"],
            expected["num_stations_lines"],
            f"num_stations_lines mismatch: expected {expected['num_stations_lines']} observed {observed['num_stations_lines']}",
        )


if __name__ == "__main__":
    unittest.main()
