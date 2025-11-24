import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.schemas import Meta, Detection, DetectionCore, Annotation
from eewpw_parser.sinks import JsonlStreamSink


class TestJsonlStreamSink(unittest.TestCase):
    def test_stream_sink_writes_jsonl_records(self):
        with tempfile.TemporaryDirectory() as td:
            out_path = Path(td) / "out.jsonl"
            sink = JsonlStreamSink(out_path, algo="vs", dialect="scvs", instance="inst1")
            sink.start_run()

            core = DetectionCore(
                id="e1",
                mag="1.0",
                lat="0.0",
                lon="0.0",
                depth="0.0",
                orig_time="2020-01-01T00:00:00Z",
            )
            d1 = Detection(
                timestamp="2020-01-01T00:00:01Z",
                event_id="e1",
                category="live",
                instance="inst1",
                orig_sys="vs",
                version="1",
                core_info=core,
                fault_info=[],
                gm_info={"pgv_obs": [], "pga_obs": []},
            )
            ann1 = Annotation(timestamp="2020-01-01T00:00:00Z", pattern="p", line="1", text="t1", pattern_id="pid")

            sink.emit_detection(d1)
            sink.emit_annotation("profile", ann1)
            meta = Meta(algo="vs", dialect="scvs", stats_total={}, extras={})
            sink.finalize(meta)

            lines = out_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 3)
            parsed = [json.loads(l) for l in lines]
            for rec in parsed:
                self.assertIn("record_type", rec)
                self.assertIn("algo", rec)
                self.assertIn("dialect", rec)
                self.assertIn("instance", rec)
                self.assertIn("payload", rec)
            self.assertEqual(parsed[-1]["record_type"], "meta")


if __name__ == "__main__":
    unittest.main()
