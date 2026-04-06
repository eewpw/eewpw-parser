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


class TestCLIShowEnv(unittest.TestCase):
    @staticmethod
    def _section_for(output: str, heading: str) -> str:
        after = output.split(f"{heading}\n", 1)[1]
        return after.split("\n\n", 1)[0]

    def test_show_env_runs_without_parse_inputs_or_required_flags(self):
        result = subprocess.run(CLI + ["--show-env"], capture_output=True, text=True, env=ENV)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("Python", result.stdout)
        self.assertIn("Package", result.stdout)
        self.assertIn("Config lookup order", result.stdout)
        self.assertIn("Resolved files", result.stdout)
        self.assertIn(
            "global.json\n[ ] --config-root (not set)\n[ ] EEWPW_PARSER_CONFIG_ROOT (not set)\n[x] packaged defaults",
            result.stdout,
        )

    def test_show_env_uses_compact_vs_expanded_file_format(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            (cfg_root / "profiles").mkdir(parents=True, exist_ok=True)
            (cfg_root / "global.json").write_text("{}", encoding="utf-8")
            (cfg_root / "profiles" / "vs_time_vs_mag.json").write_text("{}", encoding="utf-8")

            result = subprocess.run(
                CLI + ["--show-env", "--config-root", str(cfg_root)],
                capture_output=True,
                text=True,
                env=ENV,
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)

            global_block = self._section_for(result.stdout, "global.json")
            self.assertEqual(global_block.strip(), "[x] --config-root")

            vs_block = self._section_for(result.stdout, "profiles/vs_time_vs_mag.json")
            self.assertEqual(vs_block.strip(), "[x] --config-root")

            gfast_block = self._section_for(result.stdout, "profiles/gfast_time_vs_mag.json")
            self.assertIn(
                f"[ ] --config-root {cfg_root / 'profiles' / 'gfast_time_vs_mag.json'} (missing)",
                gfast_block,
            )
            self.assertIn("[ ] EEWPW_PARSER_CONFIG_ROOT (not set)", gfast_block)
            self.assertIn("[x] packaged defaults", gfast_block)
            self.assertNotIn("winner:", result.stdout)

    def test_show_env_config_lookup_paths_are_absolute(self):
        with tempfile.TemporaryDirectory() as td:
            cwd = Path(td)
            cfg_root = cwd / "cfg"
            (cfg_root / "profiles").mkdir(parents=True, exist_ok=True)
            (cfg_root / "global.json").write_text("{}", encoding="utf-8")

            result = subprocess.run(
                CLI + ["--show-env", "--config-root", "cfg"],
                capture_output=True,
                text=True,
                env=ENV,
                cwd=str(cwd),
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn(f"path      : {cfg_root.resolve()}", result.stdout)

    def test_show_env_rejects_invalid_config_root(self):
        with tempfile.TemporaryDirectory() as td:
            invalid_root = Path(td) / "missing-dir"
            result = subprocess.run(
                CLI + ["--show-env", "--config-root", str(invalid_root)],
                capture_output=True,
                text=True,
                env=ENV,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("--config-root must be an existing directory", result.stderr)


if __name__ == "__main__":
    unittest.main()
