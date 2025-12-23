# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.finder.finder_parser import FinderParser

EXAMPLE_LOG = ROOT / "example-data" / "Elm2020" / "scfinder.log"


def _parse_scfinder():
    cfg = {"dialect": "scfinder", "verbose": False}
    parser = FinderParser(cfg)
    doc = parser.parse([str(EXAMPLE_LOG)])
    assert doc is not None
    return doc


class TestSCFinderSolutionDetails(unittest.TestCase):
    def test_scfinder_solution_metrics_and_origin_epoch(self):
        doc = _parse_scfinder()
        self.assertGreater(len(doc.detections), 0)
        det = next((d for d in doc.detections if d.finder_details is not None), doc.detections[0])
        fd = det.finder_details
        self.assertIsNotNone(fd)
        sm = fd.solution_metrics
        for key in [
            "mag_uncer",
            "epicenter_lat_uncer",
            "epicenter_lon_uncer",
            "depth_uncer",
            "origin_time_uncer",
            "num_stations",
            "azimuth",
        ]:
            self.assertIn(key, sm, f"missing solution_metrics[{key}]")
        self.assertTrue(fd.origin_time_epoch, "missing origin_time_epoch")

    def test_scfinder_solution_kv_and_version(self):
        doc = _parse_scfinder()
        target = next((d for d in doc.detections if d.finder_details and d.finder_details.solution), None)
        self.assertIsNotNone(target, "No detection with finder_details.solution found")
        sol = target.finder_details.solution
        expected_any = ["Thresh", "Length", "Strike", "mag", "Centroid Lat", "Centroid Lon"]
        for k in expected_any:
            self.assertIn(k, sol, f"missing solution[{k}]")
        if "Version" in sol:
            self.assertEqual(str(target.version), str(sol["Version"]))

    def test_scfinder_finder_flags_present(self):
        doc = _parse_scfinder()
        det_with_flags = next((d for d in doc.detections if d.finder_details and d.finder_details.finder_flags), None)
        self.assertIsNotNone(det_with_flags, "No detection carried finder_flags")
        flags = det_with_flags.finder_details.finder_flags or {}
        for k in ["event_continue", "hold_object", "message"]:
            self.assertIn(k, flags, f"missing finder_flags[{k}]")


if __name__ == "__main__":
    unittest.main()