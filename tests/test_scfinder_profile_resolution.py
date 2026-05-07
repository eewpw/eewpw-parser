import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser import config as parser_config
from eewpw_parser import config_loader
from eewpw_parser.parsers.finder.finder_parser import FinderParser


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_log(path: Path, messages: list[str]) -> None:
    lines = [
        f"2024/01/01 00:00:{idx:02d} [notice/Application] {msg}"
        for idx, msg in enumerate(messages)
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class TestSCFinderProfileResolution(unittest.TestCase):
    def setUp(self) -> None:
        config_loader.set_config_root_override(None)
        parser_config.load_profile.cache_clear()
        os.environ.pop("EEWPW_PARSER_CONFIG_ROOT", None)

    def tearDown(self) -> None:
        config_loader.set_config_root_override(None)
        parser_config.load_profile.cache_clear()
        os.environ.pop("EEWPW_PARSER_CONFIG_ROOT", None)

    def _parse_annotations(self, log_path: Path):
        parser = FinderParser({"dialect": "scfinder", "verbose": False})
        doc = parser.parse([str(log_path)])
        return doc.annotations["time_vs_magnitude"]

    def test_scfinder_resolves_scfinder_profile_filename(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            _write_json(
                cfg_root / "annotations.json",
                {"annotations": {"time_vs_magnitude": {}}},
            )
            _write_json(
                cfg_root / "profiles" / "scfinder_time_vs_mag.json",
                {
                    "algorithm": "finder",
                    "dialect": "scfinder",
                    "patterns": {
                        "42": "SCFINDER_PROFILE_MARKER",
                    },
                },
            )
            _write_json(
                cfg_root / "profiles" / "finder_time_vs_mag.json",
                {
                    "algorithm": "finder",
                    "dialect": "finder",
                    "patterns": {
                        "99": "FINDER_PROFILE_MARKER",
                    },
                },
            )
            log_path = cfg_root / "scfinder.log"
            _write_log(
                log_path,
                [
                    "SCFINDER_PROFILE_MARKER",
                    "FINDER_PROFILE_MARKER",
                ],
            )

            config_loader.set_config_root_override(cfg_root)
            parser_config.load_profile.cache_clear()

            anns = self._parse_annotations(log_path)
            pattern_ids = {a.pattern_id for a in anns}
            self.assertIn("finder/scfinder:42", pattern_ids)
            self.assertNotIn("finder/scfinder:99", pattern_ids)

    def test_scfinder_ignores_finder_profile_override_only(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            _write_json(
                cfg_root / "annotations.json",
                {"annotations": {"time_vs_magnitude": {}}},
            )
            _write_json(
                cfg_root / "profiles" / "finder_time_vs_mag.json",
                {
                    "algorithm": "finder",
                    "dialect": "finder",
                    "patterns": {
                        "901": "FINDER_ONLY_OVERRIDE_MARKER",
                    },
                },
            )
            log_path = cfg_root / "scfinder.log"
            _write_log(
                log_path,
                [
                    "FINDER_ONLY_OVERRIDE_MARKER",
                    "length has decreased",
                ],
            )

            config_loader.set_config_root_override(cfg_root)
            parser_config.load_profile.cache_clear()

            anns = self._parse_annotations(log_path)
            pattern_ids = {a.pattern_id for a in anns}
            self.assertNotIn("finder/scfinder:901", pattern_ids)
            self.assertGreater(len(pattern_ids), 0)

    def test_example_configs_include_scfinder_profile(self):
        scfinder_profile = ROOT / "example-configs" / "profiles" / "scfinder_time_vs_mag.json"
        self.assertTrue(scfinder_profile.exists())

        profile = json.loads(scfinder_profile.read_text(encoding="utf-8"))
        self.assertEqual(profile.get("dialect"), "scfinder")
        self.assertIn("patterns", profile)
        self.assertIn("1", profile["patterns"])
        self.assertIn("2", profile["patterns"])


if __name__ == "__main__":
    unittest.main()
