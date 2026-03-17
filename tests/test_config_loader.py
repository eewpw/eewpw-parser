import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser import config_loader
from eewpw_parser import config as parser_config


class TestConfigLoader(unittest.TestCase):
    def tearDown(self) -> None:
        config_loader.set_config_root_override(None)
        parser_config.load_profile.cache_clear()
        os.environ.pop("EEWPW_PARSER_CONFIG_ROOT", None)

    def test_packaged_defaults_readable(self):
        config_loader.set_config_root_override(None)

        global_cfg = config_loader.open_config_json("global.json")
        self.assertIn("output", global_cfg)
        self.assertEqual(global_cfg["output"]["indent"], 2)

        profile = config_loader.open_config_json("profiles/vs_time_vs_mag.json")
        self.assertIn("patterns", profile)
        self.assertIn("start_event", profile["patterns"])
        self.assertIn("end_event", profile["patterns"])
        self.assertNotIn("timestamp_regex", profile["patterns"])

    def test_load_profile_strips_timestamp_regex(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            (cfg_root / "profiles").mkdir(parents=True, exist_ok=True)
            (cfg_root / "profiles" / "strip-test.json").write_text(
                json.dumps(
                    {
                        "patterns": {
                            "timestamp_regex": "(\\d+)",
                            "start_event": "Start logging for event",
                        }
                    }
                ),
                encoding="utf-8",
            )

            config_loader.set_config_root_override(cfg_root)
            parser_config.load_profile.cache_clear()
            profile = parser_config.load_profile("profiles/strip-test.json")
            self.assertIn("patterns", profile)
            self.assertIn("start_event", profile["patterns"])
            self.assertNotIn("timestamp_regex", profile["patterns"])

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

    def test_cli_override_wins_over_env(self):
        with tempfile.TemporaryDirectory() as td:
            env_root = Path(td) / "env"
            cli_root = Path(td) / "cli"
            env_root.mkdir(parents=True, exist_ok=True)
            cli_root.mkdir(parents=True, exist_ok=True)
            (env_root / "global.json").write_text(
                json.dumps({"output": {"indent": 4}}), encoding="utf-8"
            )
            (cli_root / "global.json").write_text(
                json.dumps({"output": {"indent": 7}}), encoding="utf-8"
            )

            with mock.patch.dict(os.environ, {"EEWPW_PARSER_CONFIG_ROOT": str(env_root)}):
                config_loader.set_config_root_override(cli_root)
                cfg = config_loader.open_config_json("global.json")
                self.assertEqual(cfg["output"]["indent"], 7)

    def test_no_repo_fallback_when_packaged_default_missing(self):
        with tempfile.TemporaryDirectory() as td:
            missing_file = Path(td) / "does-not-exist.json"
            config_loader.set_config_root_override(None)
            with mock.patch.dict(os.environ, {}, clear=True):
                with mock.patch(
                    "eewpw_parser.config_loader.get_package_config_path",
                    return_value=missing_file,
                ):
                    with self.assertRaises(FileNotFoundError):
                        config_loader.open_config_json("global.json")


if __name__ == "__main__":
    unittest.main()
