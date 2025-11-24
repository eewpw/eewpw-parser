import os
import sys
import tempfile
import unittest
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent
ENV = os.environ.copy()
ENV["PYTHONPATH"] = str(ROOT / "src")
CLI = [sys.executable, str(ROOT / "src" / "eewpw_parser" / "replay_log_cli.py")]


class TestReplayCLI(unittest.TestCase):
    def test_replay_writes_fake_logs_in_tmp(self):
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "sample.log"
            log_content = "2020/01/01 00:00:00 first\n2020/01/01 00:00:01 second\n"
            log_path.write_text(log_content, encoding="utf-8")
            cmd = CLI + ["--speed", "0", str(log_path)]
            result = subprocess.run(cmd, capture_output=True, text=True, env=ENV, cwd=td)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            fake_path = Path(td) / "tmp" / f"fake_{log_path.name}"
            self.assertTrue(fake_path.exists())
            self.assertEqual(fake_path.read_text(encoding="utf-8"), log_content)


if __name__ == "__main__":
    unittest.main()
