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
CLI = [sys.executable, str(ROOT / "src" / "eewpw_parser" / "cli.py")]


class TestCLIConfigOverride(unittest.TestCase):
    def test_cli_respects_config_root_override(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            (cfg_root / "global.json").write_text(
                json.dumps({"output": {"pretty": True, "indent": 3, "ensure_ascii": False}}),
                encoding="utf-8",
            )
            (cfg_root / "vs.json").write_text(
                json.dumps({"algo": "vs", "dialect": "scvsmag", "merge_multi_files": True}),
                encoding="utf-8",
            )

            log_path = ROOT / "tests" / "test-data" / "scvsmag-processing-info.log"
            out_path = Path(td) / "out.json"

            cmd = CLI + [
                "--algo",
                "vs",
                "--output",
                str(out_path),
                "--config-root",
                str(cfg_root),
                str(log_path),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, env=ENV, cwd=td)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(out_path.exists())

            lines = out_path.read_text(encoding="utf-8").splitlines()
            first_key_line = next((ln for ln in lines if ln.startswith(" ")), "")
            self.assertTrue(first_key_line.startswith("   "), msg=f"Indent not applied: {first_key_line}")


if __name__ == "__main__":
    unittest.main()
