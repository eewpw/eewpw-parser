import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.finder.finder_parser import FinderParser
from eewpw_parser.sinks import JsonlStreamSink


SYNTH_LOG_CONTENT = """\
2020/01/01 00:00:00 [notice/Application] event_id = 1
2020/01/01 00:00:01 [notice/Application] -> get_mag = 3.2
2020/01/01 00:00:02 [notice/Application] -> get_epicenter_lat = 1.0
2020/01/01 00:00:03 [notice/Application] -> get_epicenter_lon = 2.0
2020/01/01 00:00:04 [notice/Application] -> get_depth = 5.0
2020/01/01 00:00:05 [notice/Application] -> get_origin_time = 1609459205
"""


class TestFinderStreamMode(unittest.TestCase):
    def test_stream_sink_creates_jsonl(self):
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "finder.log"
            log_path.write_text(SYNTH_LOG_CONTENT, encoding="utf-8")

            jsonl_path = Path(td) / "out.jsonl"
            sink = JsonlStreamSink(jsonl_path, algo="finder", dialect="scfinder", instance="inst1")
            parser = FinderParser({"dialect": "scfinder"})
            parser.parse([str(log_path)], sink=sink)

            lines = jsonl_path.read_text(encoding="utf-8").strip().splitlines()
            # Expect 1 detection + 1 meta (no annotations)
            self.assertEqual(len(lines), 2)
            parsed = [json.loads(l) for l in lines]
            self.assertEqual(parsed[0]["record_type"], "detection")
            self.assertEqual(parsed[-1]["record_type"], "meta")

    def test_batch_path_returns_finaldoc(self):
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "finder.log"
            log_path.write_text(SYNTH_LOG_CONTENT, encoding="utf-8")
            parser = FinderParser({"dialect": "scfinder"})
            doc = parser.parse([str(log_path)], sink=None)
            self.assertIsNotNone(doc)
            self.assertEqual(len(doc.detections), 1)


if __name__ == "__main__":
    unittest.main()
