import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.sinks import CompositeSink, BaseSink
from eewpw_parser.schemas import Detection, DetectionCore, Annotation, Meta, FinalDoc


class RecordingSink(BaseSink):
    def __init__(self):
        self.started = False
        self.detections = []
        self.annotations = []
        self.meta = None

    def start_run(self) -> None:
        self.started = True

    def emit_detection(self, det: Detection) -> None:
        self.detections.append(det)

    def emit_annotation(self, profile: str, ann: Annotation) -> None:
        self.annotations.append((profile, ann))

    def finalize(self, meta: Meta) -> FinalDoc | None:
        self.meta = meta
        return None


class TestCompositeSink(unittest.TestCase):
    def test_fan_out(self):
        sink1 = RecordingSink()
        sink2 = RecordingSink()
        comp = CompositeSink([sink1, sink2])

        core = DetectionCore(
            id="e1",
            mag="1.0",
            lat="0.0",
            lon="0.0",
            depth="0.0",
            orig_time="2020-01-01T00:00:00Z",
        )
        det = Detection(
            timestamp="2020-01-01T00:00:01Z",
            event_id="e1",
            category="live",
            instance="inst",
            orig_sys="vs",
            version="1",
            core_info=core,
            fault_info=[],
            gm_info={"pgv_obs": [], "pga_obs": []},
        )
        ann = Annotation(timestamp="2020-01-01T00:00:00Z", pattern="p", line="1", text="t", pattern_id="pid")
        meta = Meta(algo="vs", dialect="scvs", stats_total={}, extras={})

        comp.start_run()
        comp.emit_detection(det)
        comp.emit_annotation("time_vs_magnitude", ann)
        comp.finalize(meta)

        for s in (sink1, sink2):
            self.assertTrue(s.started)
            self.assertEqual(s.detections, [det])
            self.assertEqual(s.annotations, [("time_vs_magnitude", ann)])
            self.assertEqual(s.meta, meta)


if __name__ == "__main__":
    unittest.main()
