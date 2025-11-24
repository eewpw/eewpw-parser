import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV = os.environ.copy()
ENV["PYTHONPATH"] = str(ROOT / "src")
CLI = [sys.executable, str(ROOT / "src" / "eewpw_parser" / "replay_cli.py")]

SAMPLE_VS_LOG = Path(__file__).resolve().parent / "test-data/scvsmag-processing-info.log"


class TestReplayCLI(unittest.TestCase):
    def test_replay_jsonl_contains_meta_with_replay_extras(self):
        with tempfile.TemporaryDirectory() as td:
            out_path = Path(td) / "out.jsonl"
            cmd = CLI + [
                "--algo",
                "vs",
                "--dialect",
                "scvs",
                "--speed",
                "100.0",
                "-o",
                str(out_path),
                str(SAMPLE_VS_LOG),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, env=ENV)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(out_path.exists())
            lines = out_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertGreaterEqual(len(lines), 2)
            meta_rec = json.loads(lines[-1])
            self.assertEqual(meta_rec.get("record_type"), "meta")
            extras = meta_rec.get("payload", {}).get("extras", {})
            self.assertIn("replay", extras)


if __name__ == "__main__":
    unittest.main()
