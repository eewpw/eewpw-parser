import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.schemas import Meta, Detection, DetectionCore, Annotation
from eewpw_parser.sinks import FinalDocSink


class TestFinalDocSink(unittest.TestCase):
    def test_finalize_dedup_and_sort(self):
        sink = FinalDocSink()
        sink.start_run()

        core = DetectionCore(
            id="e1",
            mag="5.0",
            lat="1.0",
            lon="2.0",
            depth="3.0",
            orig_time="2020-01-01T00:00:00Z",
            likelihood="0.9",
        )
        d1 = Detection(
            timestamp="2020-01-01T00:00:02Z",
            event_id="e1",
            category="live",
            instance="sink@test",
            orig_sys="vs",
            version="1",
            core_info=core,
            fault_info=[],
            gm_info={"pgv_obs": [], "pga_obs": []},
        )
        d2 = Detection(
            timestamp="2020-01-01T00:00:01Z",
            event_id="e1",
            category="live",
            instance="sink@test",
            orig_sys="vs",
            version="1",
            core_info=core,
            fault_info=[],
            gm_info={"pgv_obs": [], "pga_obs": []},
        )
        d3 = Detection(
            timestamp="2020-01-01T00:00:01Z",
            event_id="e1",
            category="live",
            instance="sink@test",
            orig_sys="vs",
            version="1",
            core_info=core,
            fault_info=[],
            gm_info={"pgv_obs": [], "pga_obs": []},
        )  # duplicate of d2

        sink.emit_detection(d1)
        sink.emit_detection(d2)
        sink.emit_detection(d3)

        a1 = Annotation(timestamp="2020-01-01T00:00:00Z", pattern="p", line="1", text="t1", pattern_id="pid")
        a2 = Annotation(timestamp="2020-01-01T00:00:00Z", pattern="p", line="1", text="t1", pattern_id="pid")
        sink.emit_annotation("profile", a1)
        sink.emit_annotation("profile", a2)

        meta = Meta(algo="vs", dialect="scvs", stats_total={}, extras={})
        doc = sink.finalize(meta)

        self.assertIsNotNone(doc)
        self.assertEqual(len(doc.detections), 2)  # d2 and d3 deduped
        self.assertEqual(doc.detections[0].timestamp, "2020-01-01T00:00:01Z")  # sorted
        self.assertEqual(len(doc.annotations["profile"]), 2)  # annotations are not deduped by sink


if __name__ == "__main__":
    unittest.main()
