import json
import os
import sys
import tempfile
import unittest
from importlib import resources
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser import config_loader


class TestConfigLoader(unittest.TestCase):
    def tearDown(self) -> None:
        config_loader.set_config_root_override(None)
        os.environ.pop("EEWPW_PARSER_CONFIG_ROOT", None)

    def test_packaged_defaults_readable(self):
        pkg_root = Path(resources.files("eewpw_parser.configs"))
        config_loader.set_config_root_override(pkg_root)

        global_cfg = config_loader.open_config_json("global.json")
        self.assertIn("output", global_cfg)
        self.assertEqual(global_cfg["output"]["indent"], 2)

        profile = config_loader.open_config_json("profiles/vs_time_vs_mag.json")
        self.assertIn("patterns", profile)
        self.assertIn("timestamp_regex", profile["patterns"])

    def test_env_override_wins(self):
        with tempfile.TemporaryDirectory() as td:
            override_root = Path(td)
            (override_root / "global.json").write_text(
                json.dumps({"output": {"indent": 9}}), encoding="utf-8"
            )

            with mock.patch.dict(os.environ, {"EEWPW_PARSER_CONFIG_ROOT": td}):
                config_loader.set_config_root_override(None)
                cfg = config_loader.open_config_json("global.json")
                self.assertEqual(cfg["output"]["indent"], 9)

    def test_repo_fallback_when_no_env_or_override(self):
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "global.json").write_text(
                json.dumps({"output": {"indent": 4}}), encoding="utf-8"
            )

            with mock.patch.dict(os.environ, {}, clear=False):
                config_loader.set_config_root_override(None)
                with mock.patch("eewpw_parser.config_loader._repo_config_root", return_value=repo_root):
                    cfg = config_loader.open_config_json("global.json")
                    self.assertEqual(cfg["output"]["indent"], 4)


if __name__ == "__main__":
    unittest.main()
