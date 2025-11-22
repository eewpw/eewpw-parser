import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.vs.vs_parser import VSParser


SAMPLE_LOG = ROOT.parent / "test-data/parser_train_data/ELM2020/scvsmag-processing-info.log"


class TestVSParser(unittest.TestCase):
    def test_parse_processing_info_log(self):
        parser = VSParser({"dialect": "scvs"})
        doc = parser.parse([str(SAMPLE_LOG)])

        self.assertEqual(len(doc.detections), 30)
        first = doc.detections[0]
        self.assertEqual(first.event_id, "gfz2020uzys")
        self.assertAlmostEqual(float(first.core_info.mag), 4.83, places=2)
        self.assertTrue(first.timestamp.startswith("2020-10-25T19:35:48"))
        # Ensure GM observations captured
        self.assertGreaterEqual(len(first.gm_info.get("pga_obs", [])), 1)


if __name__ == "__main__":
    unittest.main()
