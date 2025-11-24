import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.live_engine import LiveEngine
from eewpw_parser.sources import TailLineSource
from eewpw_parser.parsers.vs.dialects import VSDialect


def write_vs_lines(p: Path):
    lines = [
        "2025/11/24 12:00:00 [processing/info/VsMagnitude] Start logging for event: EVT1\n",
        "2025/11/24 12:00:01 [processing/info/VsMagnitude] VS-mag: 4.2; median single-station-mag: 4.0; lat: 35.0; lon: -120.0; depth : 5.0\n",
        "2025/11/24 12:00:01 [processing/info/VsMagnitude] creation time: 2025-11-24 12:00:01; origin time: 2025-11-24 12:00:00;\n",
        "2025/11/24 12:00:02 [processing/info/VsMagnitude] End logging for event: EVT1\n",
    ]
    p.write_text("".join(lines), encoding="utf-8")


class TestLiveEngineBasic(unittest.TestCase):
    def test_vs_engine_single_event(self):
        with tempfile.TemporaryDirectory() as td:
            log = Path(td) / "vs.log"
            outdir = Path(td) / "out"
            write_vs_lines(log)

            source = TailLineSource(str(log), poll_interval=0.01, seek_end=False, max_lines=None, follow=False)
            parser = VSDialect()
            engine = LiveEngine(
                source=source,
                parser=parser,
                output_dir=outdir,
                algo="vs",
                dialect="scvs",
                instance="vs@test",
                verbose=False,
            )

            engine.run_forever()  # finite because follow=False
            engine.shutdown()

            self.assertTrue(outdir.exists())
            files = list(outdir.glob("*.jsonl"))
            self.assertEqual(len(files), 1)

            content = files[0].read_text(encoding="utf-8").strip().splitlines()
            self.assertGreaterEqual(len(content), 1)
            parsed = [json.loads(l) for l in content]
            self.assertTrue(any(rec.get("record_type") == "detection" for rec in parsed))
            for rec in parsed:
                self.assertEqual(rec.get("algo"), "vs")


if __name__ == "__main__":
    unittest.main()
