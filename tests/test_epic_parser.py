import json
import re
import sys
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.epic.epic_parser import EpicParser
from eewpw_parser.parsers.epic.dialects import EpicShakeAlertDialect, EpicStreamState


EPIC_LOG = ROOT / "example-log-files/epic_shakealert/epic_20251113_14.log"


class TestEpicParser(unittest.TestCase):
    def test_parse_epic_log(self):
        parser = EpicParser({"dialect": "shakealert"})
        doc = parser.parse([str(EPIC_LOG)])

        self.assertGreater(len(doc.detections), 0)
        for det in doc.detections:
            self.assertIsInstance(det.timestamp, str)
            self.assertIsInstance(det.event_id, str)
            self.assertIsInstance(det.version, str)
            self.assertIsInstance(det.core_info.mag, str)
            self.assertIsInstance(det.core_info.lat, str)
            self.assertIsInstance(det.core_info.lon, str)
            self.assertIsInstance(det.core_info.depth, str)
            self.assertIsInstance(det.core_info.orig_time, str)

            self.assertIsInstance(det.gm_info.pga_obs, list)
            self.assertIsInstance(det.gm_info.pgv_obs, list)

            for obs in det.gm_info.pga_obs + det.gm_info.pgv_obs:
                self.assertIsInstance(obs.SNCL, str)
                self.assertIsInstance(obs.value, str)
                self.assertIsInstance(obs.lat, str)
                self.assertIsInstance(obs.lon, str)
                self.assertIsInstance(obs.time, str)
                if obs.orig_sys is not None:
                    self.assertIsInstance(obs.orig_sys, str)

    def test_xml_block_extraction_count(self):
        parser = EpicParser({"dialect": "shakealert"})
        doc = parser.parse([str(EPIC_LOG)])

        dialect = EpicShakeAlertDialect()
        lines = EPIC_LOG.read_text(encoding="utf-8", errors="ignore").splitlines(True)

        in_block = False
        buf = []
        count = 0
        for line in lines:
            msg = dialect.normalize_line(line)
            if msg is None:
                continue
            if not in_block:
                if "<?xml" in msg or "<event_message" in msg:
                    in_block = True
                    buf = [msg]
                    if "</event_message>" in msg:
                        count += 1
                        buf = []
                        in_block = False
            else:
                buf.append(msg)
                if "</event_message>" in msg:
                    count += 1
                    buf = []
                    in_block = False

        self.assertEqual(count, len(doc.detections))

    def test_wall_clock_rollover_and_xml_timestamp(self):
        parser = EpicParser({"dialect": "shakealert"})
        doc = parser.parse([str(EPIC_LOG)])

        dialect = EpicShakeAlertDialect()
        d_dets, d_ann, extra = dialect.parse_file(str(EPIC_LOG))
        self.assertEqual(extra.get("rollover_count"), 1)

        lines = EPIC_LOG.read_text(encoding="utf-8", errors="ignore").splitlines(True)
        first_xml = None
        in_block = False
        buf = []
        for line in lines:
            msg = dialect.normalize_line(line)
            if msg is None:
                continue
            if not in_block:
                if "<?xml" in msg or "<event_message" in msg:
                    in_block = True
                    buf = [msg]
                    if "</event_message>" in msg:
                        first_xml = "\n".join(buf)
                        break
            else:
                buf.append(msg)
                if "</event_message>" in msg:
                    first_xml = "\n".join(buf)
                    break

        self.assertIsNotNone(first_xml)
        cleaned = re.sub(r"<\?xml[^>]*\?>", "", first_xml).strip()
        root = ET.fromstring(cleaned)
        xml_ts = root.attrib.get("timestamp", "")
        self.assertEqual(doc.detections[0].timestamp, xml_ts)

        state = EpicStreamState(start_date=dialect._extract_start_date(lines))
        first_ts = None
        for line in lines:
            first_ts = dialect._update_wall_clock(line, state)
            if first_ts:
                break

        self.assertIsNotNone(first_ts)
        self.assertTrue(first_ts.endswith("Z"))
        self.assertNotIn("+", first_ts)
        self.assertIsNone(re.search(r"[+-]\d{2}:\d{2}$", first_ts))

    def test_annotations_and_offline_output(self):
        parser = EpicParser({"dialect": "shakealert"})
        doc = parser.parse([str(EPIC_LOG)])

        self.assertIn("time_vs_magnitude", doc.annotations)
        ann_list = doc.annotations["time_vs_magnitude"]
        self.assertIsInstance(ann_list, list)

        lines = EPIC_LOG.read_text(encoding="utf-8", errors="ignore").splitlines(True)
        if ann_list:
            for ann in ann_list:
                self.assertIsInstance(ann.timestamp, str)
                self.assertTrue(ann.timestamp.endswith("Z"))
                self.assertIsInstance(ann.line, str)
                self.assertIsInstance(ann.pattern, str)
                self.assertTrue(ann.pattern)
                self.assertTrue(ann.text)
                self.assertIsInstance(ann.text, str)
                self.assertTrue(ann.pattern_id.startswith("epic/shakealert:"))

                line_no = int(ann.line)
                raw_line = lines[line_no - 1].rstrip("\n")
                self.assertEqual(ann.text, raw_line)
        else:
            self.assertEqual(ann_list, [])

        offline_dir = ROOT / "tmp" / "offline_output"
        offline_dir.mkdir(parents=True, exist_ok=True)
        out_path = offline_dir / "epic_20251113_14.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(doc.model_dump(), f, indent=2, ensure_ascii=False)

        self.assertTrue(out_path.exists())
        self.assertGreater(out_path.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
