import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.live_writer import DailyAlgoWriter
from eewpw_parser.schemas import Detection, DetectionCore


class TestLiveRotation(unittest.TestCase):
    def test_daily_rotation_creates_multiple_files(self):
        with tempfile.TemporaryDirectory() as td:
            data_root = Path(td)
            writer = DailyAlgoWriter(data_root, algo="vs", dialect="scvsmag", instance="vs@test", verbose=False)

            det1 = Detection(
                timestamp="2025-11-24T12:00:01Z",
                event_id="EVT1",
                category="live",
                instance="vs@test",
                orig_sys="vs",
                version="1",
                core_info=DetectionCore(
                    id="EVT1",
                    mag="4.1",
                    lat="0.0",
                    lon="0.0",
                    depth="5.0",
                    orig_time="2025-11-24T12:00:00Z",
                ),
                fault_info=[],
                gm_info={"pgv_obs": [], "pga_obs": []},
            )
            det2 = det1.copy(update={"timestamp": "2025-11-25T00:00:01Z", "event_id": "EVT2", "core_info": det1.core_info.copy(update={"id": "EVT2"})})

            writer.write_detection(det1)
            writer.write_detection(det2)
            writer.close()

            target_dir = data_root / "live" / "raw" / "vs"
            files = sorted(p.name for p in target_dir.glob("*.jsonl"))
            self.assertIn("2025-11-24_vs.jsonl", files)
            self.assertIn("2025-11-25_vs.jsonl", files)
            self.assertEqual(len(files), 2)


if __name__ == "__main__":
    unittest.main()
