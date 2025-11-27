import subprocess
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "cleanup_live_raw.py"


class TestCleanupLiveRaw(unittest.TestCase):
    def test_retention_deletes_old_files(self):
        with tempfile.TemporaryDirectory() as td:
            data_root = Path(td)
            algo_dir = data_root / "live" / "raw" / "vs"
            algo_dir.mkdir(parents=True, exist_ok=True)

            today = date.today()
            dates = [today - timedelta(days=offset) for offset in range(4)]
            for d in dates:
                path = algo_dir / f"{d.strftime('%Y-%m-%d')}_vs.jsonl"
                path.write_text("{}", encoding="utf-8")

            cmd = [sys.executable, str(SCRIPT), "--data-root", str(data_root), "--retention-days", "2"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, msg=result.stderr)

            remaining = {p.stem.split("_", 1)[0] for p in algo_dir.glob("*.jsonl")}
            expected_keep = {dates[0].strftime("%Y-%m-%d"), dates[1].strftime("%Y-%m-%d")}
            self.assertEqual(remaining, expected_keep)


if __name__ == "__main__":
    unittest.main()
