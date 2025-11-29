import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
import subprocess
from dateutil import parser as dtp


ROOT = Path(__file__).resolve().parent.parent
ENV = os.environ.copy()
ENV["PYTHONPATH"] = str(ROOT / "src")
CLI = [sys.executable, str(ROOT / "src" / "eewpw_parser" / "replay_log_cli.py")]


class TestReplayTimeRebase(unittest.TestCase):
    def test_time_mode_original_explicit_same_output(self):
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "two_lines.log"
            log_content = "2020/01/01 00:00:00 first\n2020/01/01 00:00:10 second\n"
            log_path.write_text(log_content, encoding="utf-8")

            cmd = CLI + ["--speed", "0", "--time-mode", "original", str(log_path)]
            result = subprocess.run(cmd, capture_output=True, text=True, env=ENV, cwd=td)

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            fake_path = Path(td) / "tmp" / f"fake_{log_path.name}"
            self.assertTrue(fake_path.exists())
            self.assertEqual(fake_path.read_text(encoding="utf-8"), log_content)

    def test_time_mode_realtime_rebases_to_now_and_preserves_deltas(self):
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "two_lines.log"
            log_content = "2020/01/01 00:00:00 first\n2020/01/01 00:00:10 second\n"
            log_path.write_text(log_content, encoding="utf-8")

            t0 = datetime.now(timezone.utc)
            cmd = CLI + ["--speed", "0", "--time-mode", "realtime", str(log_path)]
            result = subprocess.run(cmd, capture_output=True, text=True, env=ENV, cwd=td)

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            fake_path = Path(td) / "tmp" / f"fake_{log_path.name}"
            self.assertTrue(fake_path.exists())

            lines = fake_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 2)

            parsed = []
            for line in lines:
                dt = dtp.parse(line, fuzzy=True)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                parsed.append(dt)

            t_first, t_second = parsed
            self.assertLessEqual(abs((t_first - t0).total_seconds()), 10)
            self.assertEqual((t_second - t_first).total_seconds(), 10)

    def test_time_mode_realtime_repeat_cycle_offset(self):
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "three_lines.log"
            log_content = "2020-01-01 00:00:00 a\n2020-01-01 00:00:05 b\n2020-01-01 00:00:20 c\n"
            log_path.write_text(log_content, encoding="utf-8")

            cmd = CLI + ["--speed", "0", "--time-mode", "realtime", "--repeat", "2", str(log_path)]
            result = subprocess.run(cmd, capture_output=True, text=True, env=ENV, cwd=td)

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            fake_path = Path(td) / "tmp" / f"fake_{log_path.name}"
            self.assertTrue(fake_path.exists())

            lines = fake_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 6)

            parsed = []
            for line in lines:
                dt = dtp.parse(line, fuzzy=True)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                parsed.append(dt)

            earliest_original = dtp.parse("2020-01-01 00:00:00").replace(tzinfo=timezone.utc)
            latest_original = dtp.parse("2020-01-01 00:00:20").replace(tzinfo=timezone.utc)
            offset = (latest_original - earliest_original).total_seconds()

            first_cycle = parsed[:3]
            second_cycle = parsed[3:]
            self.assertEqual(len(first_cycle), 3)
            self.assertEqual(len(second_cycle), 3)

            for ts_first, ts_second in zip(first_cycle, second_cycle):
                self.assertEqual((ts_second - ts_first).total_seconds(), offset)


if __name__ == "__main__":
    unittest.main()
