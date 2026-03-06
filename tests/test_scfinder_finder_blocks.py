# -*- coding: utf-8 -*-
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.finder.finder_parser import FinderParser

SYNTH_LOG = """\
2024/01/01 00:00:00 [notice/Application] event_id = 1
2024/01/01 00:00:01 [notice/Application] Template_id = TPL-001
2024/01/01 00:00:02 [notice/Application] Finder centroid = 10.0/20.0/5.0
2024/01/01 00:00:03 [notice/Application] Finder rupture list =
2024/01/01 00:00:03 [notice/Application] 11.0/21.0/6.0
2024/01/01 00:00:04 [notice/Application] 12.0/22.0/7.0
2024/01/01 00:00:05 [notice/Application] Finder azimuth list =
2024/01/01 00:00:05 [notice/Application] 90,0.7
2024/01/01 00:00:06 [notice/Application] 180,0.2
2024/01/01 00:00:07 [notice/Application] Finder length list =
2024/01/01 00:00:07 [notice/Application] 12,0.9
2024/01/01 00:00:08 [notice/Application] Finder azimuth llk list =
2024/01/01 00:00:08 [notice/Application] 90,-1.2
2024/01/01 00:00:09 [notice/Application] Finder length llk list =
2024/01/01 00:00:09 [notice/Application] 12,-0.8
2024/01/01 00:00:10 [notice/Application] Finder strike pdf =
2024/01/01 00:00:10 [notice/Application] 10.0/20.0,0.01
2024/01/01 00:00:10 [notice/Application] 11.0/21.0,0.02
2024/01/01 00:00:11 [notice/Application] event_id = 2
2024/01/01 00:00:12 [notice/Application] Template_id =
2024/01/01 00:00:13 [notice/Application] Finder azimuth list =
2024/01/01 00:00:14 [notice/Application] Finder length list =
2024/01/01 00:00:15 [notice/Application] Finder azimuth llk list =
2024/01/01 00:00:16 [notice/Application] Finder length llk list =
2024/01/01 00:00:17 [notice/Application] Finder rupture list =
2024/01/01 00:00:18 [notice/Application] Finder moment pdf =
"""

SCFINDER_LOG = ROOT / "example-log-files" / "finder_scfinder" / "scfinder_Elm2020" / "scfinder.log"
SCFINDER_PB_LOG = ROOT / "example-log-files" / "finder_scfinder" / "scfinder_pb.log"


def _parse_synthetic():
    with tempfile.TemporaryDirectory() as td:
        log_path = Path(td) / "finder.log"
        log_path.write_text(SYNTH_LOG, encoding="utf-8")
        parser = FinderParser({"dialect": "scfinder", "verbose": False})
        doc = parser.parse([str(log_path)])
        assert doc is not None
        return doc


class TestSCFinderFinderBlocks(unittest.TestCase):
    def test_finder_blocks_parsed_and_ordered(self):
        doc = _parse_synthetic()
        self.assertEqual(len(doc.detections), 2)
        self.assertEqual([d.event_id for d in doc.detections], ["1", "2"])

        det = doc.detections[0]
        self.assertIsNotNone(det.finder_details)
        fd = det.finder_details
        self.assertEqual(fd.template_id, "TPL-001")
        self.assertIsNotNone(fd.centroid)
        self.assertEqual(fd.centroid.lat, "10.0")
        self.assertEqual(fd.centroid.lon, "20.0")
        self.assertEqual(fd.centroid.depth, "5.0")

        self.assertIsNotNone(fd.rupture_list)
        self.assertEqual(len(fd.rupture_list), 2)
        self.assertEqual(fd.rupture_list[0].lat, "11.0")
        self.assertEqual(fd.rupture_list[0].lon, "21.0")
        self.assertEqual(fd.rupture_list[0].depth, "6.0")

        self.assertIsNotNone(fd.azimuth_list)
        self.assertEqual(fd.azimuth_list[0].azimuth, "90")
        self.assertEqual(fd.azimuth_list[0].value, "0.7")
        self.assertIsNotNone(fd.length_list)
        self.assertEqual(fd.length_list[0].length, "12")
        self.assertEqual(fd.length_list[0].value, "0.9")
        self.assertIsNotNone(fd.azimuth_llk_list)
        self.assertEqual(fd.azimuth_llk_list[0].azimuth, "90")
        self.assertEqual(fd.azimuth_llk_list[0].llk, "-1.2")
        self.assertIsNotNone(fd.length_llk_list)
        self.assertEqual(fd.length_llk_list[0].length, "12")
        self.assertEqual(fd.length_llk_list[0].llk, "-0.8")

        pdf = fd.extra.get("pdf", {})
        self.assertIn("strike", pdf)
        self.assertEqual(len(pdf["strike"]), 2)
        self.assertEqual(pdf["strike"][0]["lat"], "10.0")
        self.assertEqual(pdf["strike"][0]["lon"], "20.0")
        self.assertEqual(pdf["strike"][0]["value"], "0.01")

    def test_empty_lists_and_pdf_omission(self):
        doc = _parse_synthetic()
        det = doc.detections[1]
        self.assertIsNotNone(det.finder_details)
        fd = det.finder_details
        self.assertEqual(fd.template_id, "")
        self.assertEqual(fd.rupture_list, [])
        self.assertEqual(fd.azimuth_list, [])
        self.assertEqual(fd.length_list, [])
        self.assertEqual(fd.azimuth_llk_list, [])
        self.assertEqual(fd.length_llk_list, [])
        self.assertNotIn("pdf", fd.extra)

    def test_real_logs_smoke(self):
        for log_path in [SCFINDER_LOG, SCFINDER_PB_LOG]:
            if not log_path.exists():
                raise unittest.SkipTest(f"missing local log file: {log_path}")
            parser = FinderParser({"dialect": "scfinder", "verbose": False})
            doc = parser.parse([str(log_path)])
            self.assertIsNotNone(doc)
            self.assertGreater(len(doc.detections), 0)
            det_with_solution = next(
                (d for d in doc.detections if d.finder_details and d.finder_details.solution), None
            )
            self.assertIsNotNone(det_with_solution, f"no solution fields found in {log_path}")
            det_with_fault = next((d for d in doc.detections if d.fault_info), None)
            self.assertIsNotNone(det_with_fault, f"no rupture list found in {log_path}")


if __name__ == "__main__":
    unittest.main()
