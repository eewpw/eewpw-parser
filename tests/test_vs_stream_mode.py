import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.vs.vs_parser import VSParser
from eewpw_parser.sinks import JsonlStreamSink


SAMPLE_LOG = Path(__file__).resolve().parent / "test-data/scvsmag-processing-info.log"


class TestVSStreamMode(unittest.TestCase):
    def test_stream_sink_creates_jsonl(self):
        with tempfile.TemporaryDirectory() as td:
            jsonl_path = Path(td) / "out.jsonl"
            sink = JsonlStreamSink(jsonl_path, algo="vs", dialect="scvsmag", instance="inst1")
            parser = VSParser({"dialect": "scvsmag"})
            parser.parse([str(SAMPLE_LOG)], sink=sink)

            lines = jsonl_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertGreaterEqual(len(lines), 2)
            parsed = [json.loads(l) for l in lines]
            self.assertEqual(parsed[-1]["record_type"], "meta")

    def test_batch_path_returns_finaldoc(self):
        parser = VSParser({"dialect": "scvsmag"})
        doc = parser.parse([str(SAMPLE_LOG)], sink=None)
        self.assertIsNotNone(doc)
        self.assertGreater(len(doc.detections), 0)


if __name__ == "__main__":
    unittest.main()
