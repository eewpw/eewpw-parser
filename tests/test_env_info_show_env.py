import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser import config_loader
from eewpw_parser import env_info


class TestEnvInfoShowEnv(unittest.TestCase):
    @staticmethod
    def _section_for(output: str, heading: str) -> str:
        assert f"{heading}\n" in output, f"heading not found: {heading}"
        after = output.split(f"{heading}\n", 1)[1]
        return after.split("\n\n", 1)[0]

    @staticmethod
    def _algo_block(output: str, algo: str) -> str:
        pattern = re.compile(rf"(?ms)^{re.escape(algo)}\n(.*?)(?:\n\n|\Z)")
        match = pattern.search(output)
        assert match is not None, f"algo block not found: {algo}"
        return match.group(1)

    @staticmethod
    def _annotation_line_for_key(report: str, key: str) -> str:
        section = TestEnvInfoShowEnv._section_for(report, "Annotation resolution report")
        for line in section.splitlines():
            if key in line:
                return line
        raise AssertionError(f"annotation resolution line missing for key: {key}")

    def setUp(self) -> None:
        config_loader.set_config_root_override(None)
        os.environ.pop("EEWPW_PARSER_CONFIG_ROOT", None)

    def tearDown(self) -> None:
        config_loader.set_config_root_override(None)
        os.environ.pop("EEWPW_PARSER_CONFIG_ROOT", None)

    def test_live_mode_section_not_emitted_in_current_contract(self):
        report = env_info.build_env_report()
        self.assertNotIn("Live-mode support", report)
        self.assertIn("Supported algorithms and dialects", report)
        self.assertIn("Annotation resolution report", report)
        self.assertIn("Deprecated legacy profile usage", report)

    def test_annotation_resolution_uses_annotations_json_when_key_defined(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            (cfg_root / "profiles").mkdir(parents=True, exist_ok=True)
            (cfg_root / "annotations.json").write_text(
                (
                    '{"annotations":{"time_vs_magnitude":'
                    '{"vs/scvsmag":{"start_event":"VS_CFG_MARKER"}}}}'
                ),
                encoding="utf-8",
            )
            (cfg_root / "profiles" / "vs_time_vs_mag.json").write_text(
                '{"patterns":{"start_event":"VS_LEGACY_MARKER"}}',
                encoding="utf-8",
            )

            config_loader.set_config_root_override(cfg_root)
            report = env_info.build_env_report()

            self.assertIn(
                f"winner: {str((cfg_root / 'annotations.json').resolve())}",
                report,
            )
            vs_line = self._annotation_line_for_key(report, "vs/scvsmag")
            self.assertIn("annotations.json", vs_line)
            plum_line = self._annotation_line_for_key(report, "plum/plum")
            self.assertIn("legacy profile", plum_line)
            self.assertIn("not defined in annotations.json", plum_line)

    def test_annotation_resolution_reports_legacy_when_annotations_missing(self):
        original_get_package_config_path = config_loader.get_package_config_path

        with tempfile.TemporaryDirectory() as td:
            missing_annotations = Path(td) / "__missing_annotations__.json"

            def _missing_packaged_annotations(rel_path: str) -> Path:
                if rel_path == "annotations.json":
                    return missing_annotations
                return original_get_package_config_path(rel_path)

            with mock.patch(
                "eewpw_parser.config_loader.get_package_config_path",
                side_effect=_missing_packaged_annotations,
            ):
                report = env_info.build_env_report()

            self.assertIn(
                "target 'time_vs_magnitude': unavailable (annotations.json missing)",
                report,
            )
            finder_line = self._annotation_line_for_key(report, "finder/scfinder")
            self.assertIn("legacy profile", finder_line)
            self.assertIn("annotations.json missing", finder_line)

    def test_supported_algorithms_section_is_user_facing(self):
        report = env_info.build_env_report()

        self.assertNotIn("source   :", report)
        self.assertNotIn("workers  :", report)
        self.assertNotIn("pass-through", report)

        finder_block = self._algo_block(report, "finder")
        self.assertIn("canonical dialects:", finder_block)
        self.assertIn("accepted inputs (alias -> canonical):", finder_block)
        self.assertRegex(
            finder_block,
            r"(?m)^\s*finder\s*\.*\s*->\s*native_finder\s*$",
        )
        self.assertRegex(
            finder_block,
            r"(?m)^\s*native-finder\s*\.*\s*->\s*native_finder\s*$",
        )
        self.assertRegex(
            finder_block,
            r"(?m)^\s*finderlegacy\s*\.*\s*->\s*native_finder_legacy\s*$",
        )
        self.assertRegex(
            finder_block,
            r"(?m)^\s*scfinder\s*\.*\s*->\s*scfinder\s*$",
        )

        vs_block = self._algo_block(report, "vs")
        self.assertRegex(
            vs_block,
            r"(?i)accepted input\s*:\s*.*restrict",
        )
        self.assertIn("canonical dialects: scvsmag", vs_block)


if __name__ == "__main__":
    unittest.main()
