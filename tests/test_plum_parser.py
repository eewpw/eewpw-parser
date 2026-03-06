import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.plum.plum_parser import PlumParser


PLUM_DIR = ROOT / "example-log-files/plum_shakealert/plum_out_20251113_14"
SAMPLE_XMLS = [
    PLUM_DIR / "2025111420355900_3125.TK.--_0.xml",
    PLUM_DIR / "2025111420483400_0131.TK.--_0.xml",
]


class TestPlumParser(unittest.TestCase):
    def _assert_obs_grid_strings(self, det):
        self.assertIsInstance(det.gm_info.pga_obs, list)
        self.assertIsInstance(det.gm_info.grid_data, list)

        for obs in det.gm_info.pga_obs:
            self.assertIsInstance(obs.SNCL, str)
            self.assertIsInstance(obs.value, str)
            self.assertIsInstance(obs.lat, str)
            self.assertIsInstance(obs.lon, str)
            self.assertIsInstance(obs.time, str)
            if obs.orig_sys is not None:
                self.assertIsInstance(obs.orig_sys, str)

        for cell in det.gm_info.grid_data:
            self.assertIsInstance(cell.lon, str)
            self.assertIsInstance(cell.lat, str)
            self.assertIsInstance(cell.pga, str)
            self.assertIsInstance(cell.pgv, str)
            self.assertIsInstance(cell.mmi, str)

    def _assert_event_id_sequence(self, dets):
        event_seq = 0
        seen_any_block = False
        for det in dets:
            self.assertIsInstance(det.event_id, str)
            if det.version == "0":
                if seen_any_block:
                    event_seq += 1
            if not seen_any_block:
                seen_any_block = True
            self.assertEqual(det.event_id, str(event_seq))

    def test_parse_directory(self):
        parser = PlumParser({"dialect": "plum"})
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp") as td:
            temp_dir = Path(td)
            for src in SAMPLE_XMLS:
                shutil.copy(src, temp_dir / src.name)

            doc = parser.parse([str(temp_dir)])

            self.assertGreater(len(doc.detections), 0)
            for det in doc.detections:
                self._assert_obs_grid_strings(det)
            self._assert_event_id_sequence(doc.detections)

    def test_parse_merged_xml_and_offline_output(self):
        parser = PlumParser({"dialect": "plum"})
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp") as td:
            temp_dir = Path(td)
            merged_path = temp_dir / "merged_plum.xml"
            with open(merged_path, "w", encoding="utf-8") as f:
                for src in SAMPLE_XMLS:
                    f.write(src.read_text(encoding="utf-8"))
                    f.write("\n")

            doc = parser.parse([str(merged_path)])

            self.assertGreater(len(doc.detections), 0)
            for det in doc.detections:
                self._assert_obs_grid_strings(det)
            self._assert_event_id_sequence(doc.detections)

            offline_dir = ROOT / "tmp" / "offline_output"
            offline_dir.mkdir(parents=True, exist_ok=True)
            out_path = offline_dir / "plum_merged_output.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(doc.model_dump(), f, indent=2, ensure_ascii=False)
            self.assertTrue(out_path.exists())
            self.assertGreater(out_path.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
