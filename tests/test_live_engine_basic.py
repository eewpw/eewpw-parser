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
from eewpw_parser.parsers.plum.plum_parser import PlumParser


def write_vs_lines(p: Path):
    lines = [
        "2025/11/24 12:00:00 [processing/info/VsMagnitude] Start logging for event: EVT1\n",
        "2025/11/24 12:00:01 [processing/info/VsMagnitude] VS-mag: 4.2; median single-station-mag: 4.0; lat: 35.0; lon: -120.0; depth : 5.0\n",
        "2025/11/24 12:00:01 [processing/info/VsMagnitude] creation time: 2025-11-24 12:00:01; origin time: 2025-11-24 12:00:00;\n",
        "2025/11/24 12:00:01 [processing/info/VsMagnitude] likelihood: 0.95\n",
        "2025/11/24 12:00:02 [processing/info/VsMagnitude] End logging for event: EVT1\n",
    ]
    p.write_text("".join(lines), encoding="utf-8")


class TestLiveEngineBasic(unittest.TestCase):
    def test_vs_engine_single_event(self):
        with tempfile.TemporaryDirectory() as td:
            log = Path(td) / "vs.log"
            data_root = Path(td) / "data_root"
            write_vs_lines(log)

            source = TailLineSource(str(log), poll_interval=0.01, seek_end=False, max_lines=None, follow=False)
            parser = VSDialect()
            engine = LiveEngine(
                source=source,
                parser=parser,
                data_root=data_root,
                algo="vs",
                dialect="scvsmag",
                instance="vs@test",
                verbose=False,
            )

            engine.run_forever()  # finite because follow=False
            engine.shutdown()

            target_dir = data_root / "live" / "raw" / "vs"
            self.assertTrue(target_dir.exists())
            files = list(target_dir.glob("*.jsonl"))
            self.assertEqual(len(files), 1)

            content = files[0].read_text(encoding="utf-8").strip().splitlines()
            self.assertGreaterEqual(len(content), 1)
            parsed = [json.loads(l) for l in content]
            self.assertTrue(any(rec.get("record_type") == "detection" for rec in parsed))
            for rec in parsed:
                self.assertEqual(rec.get("algo"), "vs")
                self.assertIn("event_id", rec)
                self.assertIn("timestamp", rec)

    def test_plum_live_timestampless_annotation_does_not_crash(self):
        with tempfile.TemporaryDirectory() as td:
            log = Path(td) / "plum.log"
            data_root = Path(td) / "data_root"
            log.write_text("Start logging for event E1\n", encoding="utf-8")

            source = TailLineSource(str(log), poll_interval=0.01, seek_end=False, max_lines=None, follow=False)
            parser = PlumParser({"dialect": "plum", "verbose": False})
            engine = LiveEngine(
                source=source,
                parser=parser,
                data_root=data_root,
                algo="plum",
                dialect="plum",
                instance="plum@test",
                verbose=False,
            )

            engine.run_forever()

            target_dir = data_root / "live" / "raw" / "plum"
            files = list(target_dir.glob("*.jsonl"))
            self.assertEqual(len(files), 1)
            records = [json.loads(line) for line in files[0].read_text(encoding="utf-8").splitlines()]
            self.assertFalse(any(rec.get("record_type") == "annotation" for rec in records))

    def test_plum_live_annotation_uses_detection_timestamp_context(self):
        with tempfile.TemporaryDirectory() as td:
            log = Path(td) / "plum.log"
            data_root = Path(td) / "data_root"
            log.write_text(
                "\n".join(
                    [
                        "<event_message timestamp=\"2025-11-14T00:00:01.000Z\" category=\"live\" instance=\"x\" orig_sys=\"sa\" version=\"0\">",
                        "<core_info id=\"1\"></core_info>",
                        "</event_message>",
                        "likelihood: 0.95",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            source = TailLineSource(str(log), poll_interval=0.01, seek_end=False, max_lines=None, follow=False)
            parser = PlumParser({"dialect": "plum", "verbose": False})
            engine = LiveEngine(
                source=source,
                parser=parser,
                data_root=data_root,
                algo="plum",
                dialect="plum",
                instance="plum@test",
                verbose=False,
            )

            engine.run_forever()

            target_dir = data_root / "live" / "raw" / "plum"
            files = list(target_dir.glob("*.jsonl"))
            self.assertEqual(len(files), 1)
            records = [json.loads(line) for line in files[0].read_text(encoding="utf-8").splitlines()]
            det = next(rec for rec in records if rec.get("record_type") == "detection")
            ann = next(rec for rec in records if rec.get("record_type") == "annotation")
            self.assertEqual(ann["timestamp"], det["timestamp"])

    def test_live_annotation_pattern_id_not_double_prefixed(self):
        with tempfile.TemporaryDirectory() as td:
            log = Path(td) / "vs.log"
            data_root = Path(td) / "data_root"
            write_vs_lines(log)

            source = TailLineSource(str(log), poll_interval=0.01, seek_end=False, max_lines=None, follow=False)
            parser = VSDialect()
            engine = LiveEngine(
                source=source,
                parser=parser,
                data_root=data_root,
                algo="vs",
                dialect="scvsmag",
                instance="vs@test",
                verbose=False,
            )

            engine.run_forever()

            target_dir = data_root / "live" / "raw" / "vs"
            files = list(target_dir.glob("*.jsonl"))
            self.assertEqual(len(files), 1)
            records = [json.loads(line) for line in files[0].read_text(encoding="utf-8").splitlines()]
            ann_payloads = [rec["payload"] for rec in records if rec.get("record_type") == "annotation"]
            self.assertGreater(len(ann_payloads), 0)
            for payload in ann_payloads:
                pattern_id = payload.get("pattern_id") or ""
                self.assertNotIn("vs/scvsmag:vs/scvsmag:", pattern_id)


if __name__ == "__main__":
    unittest.main()
