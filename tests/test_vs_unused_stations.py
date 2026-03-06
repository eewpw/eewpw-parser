import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.vs.vs_parser import VSParser  # noqa: E402


SAMPLE_LOG = ROOT / "tests/test-data/scvsmag-processing-info.log"


class TestVSUnusedStations(unittest.TestCase):
    def test_unused_stations_parsed(self):
        parser = VSParser({"dialect": "scvsmag"})
        doc = parser.parse([str(SAMPLE_LOG)])

        self.assertGreaterEqual(len(doc.detections), 1)
        det = doc.detections[0]
        self.assertEqual(det.vs_details.stations_not_used, ["8D.ELM1", "CH.PANIX"])

        any_non_empty = any(d.vs_details.stations_not_used for d in doc.detections)
        self.assertTrue(any_non_empty)


if __name__ == "__main__":
    unittest.main()
