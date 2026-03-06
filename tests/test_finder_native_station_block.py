import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.finder.finder_parser import FinderParser


NATIVE_DETECTION_THEN_STATIONS = """\
event_id = 1764837231
-> get_mag = 5.5
-> get_epicenter_lat = 49.0
-> get_epicenter_lon = -125.9
-> get_depth = 10.0
-> get_origin_time = 1577836800
Stations with PGA above the min threshold
SY.TOFB.ENE.-- 49.154/-125.908 -- 968 1577836817.680 include = 1
SY.TOFB.ENZ.-- 49.155/-125.909 -- 100 1577836818.000 include = 0
"""

NATIVE_STATIONS_THEN_DETECTION = """\
Stations with PGA above the min threshold
SY.TOFB.ENE.-- 49.154/-125.908 -- 968 1577836817.680 include = 1
event_id = 1764837231
-> get_mag = 5.5
-> get_epicenter_lat = 49.0
-> get_epicenter_lon = -125.9
-> get_depth = 10.0
-> get_origin_time = 1577836800
"""


class TestFinderNativeStationBlock(unittest.TestCase):
    def _parse_native(self, content: str):
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "finder_native.log"
            log_path.write_text(content, encoding="utf-8")
            parser = FinderParser({"dialect": "native_finder"})
            doc = parser.parse([str(log_path)], sink=None)
            return doc

    def test_stations_attach_to_prior_detection(self):
        doc = self._parse_native(NATIVE_DETECTION_THEN_STATIONS)
        self.assertIsNotNone(doc)
        self.assertEqual(len(doc.detections), 1)
        det = doc.detections[0]
        pga_obs = det.gm_info.pga_obs
        self.assertEqual(len(pga_obs), 1)
        obs = pga_obs[0]
        self.assertEqual(obs.SNCL, "SY.TOFB.ENE.--")
        self.assertAlmostEqual(float(obs.lat), 49.154)
        self.assertAlmostEqual(float(obs.lon), -125.908)
        self.assertEqual(float(obs.value), 968.0)
        self.assertTrue(obs.time.startswith("2020-01-01T"))

    def test_stations_pending_then_attach_to_first_detection(self):
        doc = self._parse_native(NATIVE_STATIONS_THEN_DETECTION)
        self.assertIsNotNone(doc)
        self.assertEqual(len(doc.detections), 1)
        det = doc.detections[0]
        pga_obs = det.gm_info.pga_obs
        self.assertEqual(len(pga_obs), 1)
        self.assertEqual(pga_obs[0].SNCL, "SY.TOFB.ENE.--")


if __name__ == "__main__":
    unittest.main()
