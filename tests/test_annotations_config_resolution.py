import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser import config as parser_config
from eewpw_parser import config_loader
from eewpw_parser.parsers.finder.finder_parser import FinderParser


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


class TestAnnotationsConfigResolution(unittest.TestCase):
    def setUp(self) -> None:
        config_loader.set_config_root_override(None)
        parser_config.load_profile.cache_clear()
        os.environ.pop("EEWPW_PARSER_CONFIG_ROOT", None)

    def tearDown(self) -> None:
        config_loader.set_config_root_override(None)
        parser_config.load_profile.cache_clear()
        os.environ.pop("EEWPW_PARSER_CONFIG_ROOT", None)

    def test_resolves_annotations_from_annotations_json(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            _write_json(
                cfg_root / "annotations.json",
                {
                    "annotations": {
                        "time_vs_magnitude": {
                            "finder/scfinder": {"42": "ANNOTATIONS_CONFIG_MARKER"},
                        }
                    }
                },
            )
            _write_json(
                cfg_root / "profiles" / "scfinder_time_vs_mag.json",
                {"patterns": {"99": "LEGACY_PROFILE_MARKER"}},
            )

            config_loader.set_config_root_override(cfg_root)
            parser_config.load_profile.cache_clear()

            profile = parser_config.load_profile(
                "profiles/scfinder_time_vs_mag.json",
                algo="finder",
                dialect="scfinder",
                target="time_vs_magnitude",
            )
            self.assertEqual(
                profile.get("patterns"),
                {"42": "ANNOTATIONS_CONFIG_MARKER"},
            )

    def test_finder_aliases_normalize_for_annotation_lookup(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            _write_json(
                cfg_root / "annotations.json",
                {
                    "annotations": {
                        "time_vs_magnitude": {
                            "finder/native_finder": {"1": "FINDER_ALIAS_MARKER"},
                        }
                    }
                },
            )
            config_loader.set_config_root_override(cfg_root)

            aliases = [
                "native_finder",
                "native-finder",
                "nativefinder",
                "finder",
            ]
            for alias in aliases:
                parser_config.load_profile.cache_clear()
                profile = parser_config.load_profile(
                    "profiles/finder_time_vs_mag.json",
                    algo="finder",
                    dialect=alias,
                    target="time_vs_magnitude",
                )
                self.assertEqual(profile.get("patterns"), {"1": "FINDER_ALIAS_MARKER"})

    def test_vs_and_plum_dialects_normalize_for_annotation_lookup(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            _write_json(
                cfg_root / "annotations.json",
                {
                    "annotations": {
                        "time_vs_magnitude": {
                            "vs/scvsmag": {"start_event": "VS_MARKER"},
                            "plum/plum": {"start_event": "PLUM_MARKER"},
                        }
                    }
                },
            )
            config_loader.set_config_root_override(cfg_root)
            parser_config.load_profile.cache_clear()

            vs_profile = parser_config.load_profile(
                "profiles/vs_time_vs_mag.json",
                algo="vs",
                dialect="custom_vs_runtime_dialect",
                target="time_vs_magnitude",
            )
            plum_profile = parser_config.load_profile(
                "profiles/plum_time_vs_mag.json",
                algo="plum",
                dialect="custom_plum_runtime_dialect",
                target="time_vs_magnitude",
            )

            self.assertEqual(vs_profile.get("patterns"), {"start_event": "VS_MARKER"})
            self.assertEqual(plum_profile.get("patterns"), {"start_event": "PLUM_MARKER"})

    def test_falls_back_to_legacy_profiles_when_annotations_json_missing(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            _write_json(
                cfg_root / "profiles" / "scfinder_time_vs_mag.json",
                {"patterns": {"11": "LEGACY_MARKER"}},
            )
            config_loader.set_config_root_override(cfg_root)
            parser_config.load_profile.cache_clear()

            original_get_package_config_path = config_loader.get_package_config_path

            def _missing_packaged_annotations(rel_path: str) -> Path:
                if rel_path == "annotations.json":
                    return cfg_root / "__packaged_annotations_missing__.json"
                return original_get_package_config_path(rel_path)

            with mock.patch(
                "eewpw_parser.config_loader.get_package_config_path",
                side_effect=_missing_packaged_annotations,
            ):
                profile = parser_config.load_profile(
                    "profiles/scfinder_time_vs_mag.json",
                    algo="finder",
                    dialect="scfinder",
                    target="time_vs_magnitude",
                )

            self.assertEqual(profile.get("patterns"), {"11": "LEGACY_MARKER"})

    def test_falls_back_to_legacy_profiles_when_target_missing(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            _write_json(
                cfg_root / "annotations.json",
                {
                    "annotations": {
                        "other_target": {
                            "finder/scfinder": {"21": "UNUSED"},
                        }
                    }
                },
            )
            _write_json(
                cfg_root / "profiles" / "scfinder_time_vs_mag.json",
                {"patterns": {"22": "LEGACY_TARGET_FALLBACK_MARKER"}},
            )
            config_loader.set_config_root_override(cfg_root)
            parser_config.load_profile.cache_clear()

            profile = parser_config.load_profile(
                "profiles/scfinder_time_vs_mag.json",
                algo="finder",
                dialect="scfinder",
                target="time_vs_magnitude",
            )
            self.assertEqual(
                profile.get("patterns"),
                {"22": "LEGACY_TARGET_FALLBACK_MARKER"},
            )

    def test_falls_back_to_legacy_profiles_when_algo_dialect_key_missing(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            _write_json(
                cfg_root / "annotations.json",
                {
                    "annotations": {
                        "time_vs_magnitude": {
                            "finder/native_finder": {"31": "UNUSED"},
                        }
                    }
                },
            )
            _write_json(
                cfg_root / "profiles" / "scfinder_time_vs_mag.json",
                {"patterns": {"32": "LEGACY_KEY_FALLBACK_MARKER"}},
            )
            config_loader.set_config_root_override(cfg_root)
            parser_config.load_profile.cache_clear()

            profile = parser_config.load_profile(
                "profiles/scfinder_time_vs_mag.json",
                algo="finder",
                dialect="scfinder",
                target="time_vs_magnitude",
            )
            self.assertEqual(
                profile.get("patterns"),
                {"32": "LEGACY_KEY_FALLBACK_MARKER"},
            )

    def test_output_shape_and_meta_dialect_remain_unchanged(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            _write_json(
                cfg_root / "annotations.json",
                {
                    "annotations": {
                        "time_vs_magnitude": {
                            "finder/native_finder": {"1": "ANNOTATION_NATIVE_MARKER"},
                        }
                    }
                },
            )
            log_path = cfg_root / "finder.log"
            log_path.write_text(
                "2024/01/01 00:00:00 [notice/Application] ANNOTATION_NATIVE_MARKER\n",
                encoding="utf-8",
            )

            config_loader.set_config_root_override(cfg_root)
            parser_config.load_profile.cache_clear()

            parser = FinderParser({"dialect": "native-finder", "verbose": False})
            doc = parser.parse([str(log_path)])

            self.assertEqual(set(doc.model_dump().keys()), {"meta", "annotations", "detections"})
            self.assertIn("time_vs_magnitude", doc.annotations)
            self.assertEqual(doc.meta.dialect, "native-finder")
            self.assertIn("finder/native-finder:1", {a.pattern_id for a in doc.annotations["time_vs_magnitude"]})


if __name__ == "__main__":
    unittest.main()
