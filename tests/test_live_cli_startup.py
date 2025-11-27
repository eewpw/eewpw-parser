import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLI = [sys.executable, str(ROOT / "src" / "eewpw_parser" / "live_cli.py")]
ENV = os.environ.copy()
ENV["PYTHONPATH"] = str(ROOT / "src")


class TestLiveCLIStartup(unittest.TestCase):
    def test_live_cli_start_and_interrupt(self):
        with tempfile.TemporaryDirectory() as td:
            log = Path(td) / "vs.log"
            data_root = Path(td) / "out"
            log.write_text("2025/11/24 12:00:00 [processing/info/VsMagnitude] Start logging for event: EVT1\n", encoding="utf-8")

            cmd = CLI + [
                "--algo", "vs",
                "--dialect", "scvsmag",
                "--instance", "vs@test",
                "--logfile", str(log),
                "--data-root", str(data_root),
                "--poll-interval", "0.01",
            ]
            proc = subprocess.Popen(cmd, env=ENV)

            # Append a couple of lines, then stop the process
            time.sleep(0.1)
            with log.open("a", encoding="utf-8") as fh:
                fh.write("2025/11/24 12:00:01 [processing/info/VsMagnitude] End logging for event: EVT1\n")
                fh.flush()

            time.sleep(0.2)
            os.kill(proc.pid, signal.SIGINT)
            proc.wait(timeout=5)

            self.assertEqual(proc.returncode, 0)
            target_dir = data_root / "live" / "raw" / "vs"
            self.assertTrue(target_dir.exists())
            self.assertGreater(len(list(target_dir.glob("*.jsonl"))), 0)


if __name__ == "__main__":
    unittest.main()
