import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.live_writer import DailyAlgoWriter
from eewpw_parser.schemas import Detection, DetectionCore, Annotation, Meta


class TestLiveDailyVS(unittest.TestCase):
    def test_daily_writer_outputs_vs_records(self):
        with tempfile.TemporaryDirectory() as td:
            data_root = Path(td)
            writer = DailyAlgoWriter(data_root, algo="vs", dialect="scvsmag", instance="vs@test", verbose=False)

            det = Detection(
                timestamp="2025-11-24T12:00:01Z",
                event_id="EVT1",
                category="live",
                instance="vs@test",
                orig_sys="vs",
                version="1",
                core_info=DetectionCore(
                    id="EVT1",
                    mag="4.2",
                    lat="35.0",
                    lon="-120.0",
                    depth="5.0",
                    orig_time="2025-11-24T12:00:00Z",
                    likelihood="0.9",
                ),
                fault_info=[],
                gm_info={"pgv_obs": [], "pga_obs": []},
            )
            ann = Annotation(
                timestamp="2025-11-24T12:00:02Z",
                pattern="time_vs_magnitude",
                line="1",
                text="annotation text",
                pattern_id=None,
            )
            meta = Meta(
                algo="vs",
                dialect="scvsmag",
                files=None,
                started_at=det.timestamp,
                finished_at=det.timestamp,
                playback_time=None,
                extras={},
                stats_total={},
            )

            writer.write_detection(det)
            writer.write_annotation("time_vs_magnitude", ann, det.event_id)
            writer.write_meta(meta)
            writer.close()

            target = data_root / "live" / "raw" / "vs" / "2025-11-24_vs.jsonl"
            self.assertTrue(target.exists())
            lines = [json.loads(l) for l in target.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(lines[0]["record_type"], "detection")
            self.assertEqual(lines[0]["event_id"], "EVT1")
            self.assertEqual(lines[1]["record_type"], "annotation")
            self.assertEqual(lines[1]["profile"], "time_vs_magnitude")
            self.assertEqual(lines[-1]["record_type"], "meta")


if __name__ == "__main__":
    unittest.main()
