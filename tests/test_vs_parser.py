import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.vs.vs_parser import VSParser


SAMPLE_LOG = ROOT / "tests/test-data/scvsmag-processing-info.log"


class TestVSParser(unittest.TestCase):
    def test_parse_scvsmag_log(self):
        parser = VSParser({"dialect": "scvsmag"})
        doc = parser.parse([str(SAMPLE_LOG)])

        self.assertEqual(len(doc.detections), 30)
        first = doc.detections[0]
        self.assertEqual(first.event_id, "gfz2020uzys")
        self.assertAlmostEqual(float(first.core_info.mag), 4.83, places=2)
        self.assertTrue(first.timestamp.startswith("2020-10-25T19:35:48"))
        # Ensure GM observations captured
        self.assertGreaterEqual(len(first.gm_info.pga_obs), 1)
        # Median single-station magnitude captured (may be None if absent)
        self.assertTrue(hasattr(first.core_info, "vs_median_single_station_mag"))
        self.assertGreaterEqual(len(first.vs_details.stations_not_used), 1)

    def test_vs_median_numeric_and_nan(self):
        parser = VSParser({"dialect": "scvsmag"})
        text_numeric = (
            "2025/11/24 12:00:01 [processing/info/VsMagnitude] "
            "VS-mag: 4.2; median single-station-mag: 5.23; lat: 35.0; lon: -120.0; depth : 5.0\n"
        )
        text_nan = (
            "2025/11/24 12:00:02 [processing/info/VsMagnitude] "
            "VS-mag: 4.2; median single-station-mag: nan; lat: 35.0; lon: -120.0; depth : 5.0\n"
        )
        sample_path = Path(SAMPLE_LOG)
        temp_content = (
            "2025/11/24 12:00:00 [processing/info/VsMagnitude] Start logging for event: test20251124\n"
            + text_numeric
            + "2025/11/24 12:00:01 [processing/info/VsMagnitude] End logging for event: test20251124\n"
            + "2025/11/24 12:00:02 [processing/info/VsMagnitude] Start logging for event: test20251124\n"
            + text_nan
            + "2025/11/24 12:00:03 [processing/info/VsMagnitude] End logging for event: test20251124\n"
        )
        temp_file = sample_path.parent / "temp_vs_mag.log"
        temp_file.write_text(temp_content, encoding="utf-8")
        try:
            doc = parser.parse([str(temp_file)])
            self.assertGreaterEqual(len(doc.detections), 2)
            
            # First detection has numeric median magnitude, second has NaN
            d1 = doc.detections[0]
            d2 = doc.detections[1]

            self.assertAlmostEqual(float(d1.core_info.vs_median_single_station_mag), 5.23, places=2)
            self.assertIsNone(d2.core_info.vs_median_single_station_mag)
            # main magnitude unaffected
            self.assertAlmostEqual(float(d1.core_info.mag), 4.2, places=1)
        finally:
            try:
                temp_file.unlink()
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    unittest.main()
