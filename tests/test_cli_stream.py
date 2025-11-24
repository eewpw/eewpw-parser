import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLI = [sys.executable, str(ROOT / "src" / "eewpw_parser" / "cli.py")]
ENV = os.environ.copy()
ENV["PYTHONPATH"] = str(ROOT / "src")

SAMPLE_VS_LOG = Path(__file__).resolve().parent / "test-data/scvsmag-processing-info.log"


class TestCLIStream(unittest.TestCase):
    def test_stream_jsonl_mode_vs(self):
        with tempfile.TemporaryDirectory() as td:
            out_path = Path(td) / "out.jsonl"
            cmd = CLI + [
                "--algo",
                "vs",
                "--mode",
                "stream-jsonl",
                "-o",
                str(out_path),
                str(SAMPLE_VS_LOG),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, env=ENV)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(out_path.exists())
            lines = out_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertGreaterEqual(len(lines), 2)
            parsed = [json.loads(l) for l in lines]
            self.assertEqual(parsed[-1]["record_type"], "meta")


if __name__ == "__main__":
    unittest.main()
