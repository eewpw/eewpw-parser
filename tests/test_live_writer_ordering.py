import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.live_writer import LiveWriter
from eewpw_parser.schemas import Detection, DetectionCore


class TestLiveWriterOrdering(unittest.TestCase):
    def test_write_order_and_flush(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "event.jsonl"
            w = LiveWriter(p, algo="vs", dialect="scvsmag", instance="vs@test")

            det1 = Detection(
                timestamp="2025-01-01T00:00:01Z",
                event_id="E1",
                category="live",
                instance="vs@test",
                orig_sys="vs",
                version="1",
                core_info=DetectionCore(
                    id="E1", mag="4.1", lat="1.0", lon="2.0", depth="5.0", orig_time="2025-01-01T00:00:00Z"
                ),
                fault_info=[],
                gm_info={"pgv_obs": [], "pga_obs": []},
            )
            det2 = Detection(
                timestamp="2025-01-01T00:00:02Z",
                event_id="E1",
                category="live",
                instance="vs@test",
                orig_sys="vs",
                version="2",
                core_info=DetectionCore(
                    id="E1", mag="4.2", lat="1.1", lon="2.1", depth="5.1", orig_time="2025-01-01T00:00:02Z"
                ),
                fault_info=[],
                gm_info={"pgv_obs": [], "pga_obs": []},
            )

            w.write_detection(det1)
            w.write_detection(det2)

            # File should be flushed and contain two lines in order
            lines = p.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 2)
            a = json.loads(lines[0])
            b = json.loads(lines[1])
            self.assertEqual(a["record_type"], "detection")
            self.assertEqual(b["record_type"], "detection")
            self.assertEqual(a["payload"]["timestamp"], "2025-01-01T00:00:01Z")
            self.assertEqual(b["payload"]["timestamp"], "2025-01-01T00:00:02Z")

            w.close()


if __name__ == "__main__":
    unittest.main()
