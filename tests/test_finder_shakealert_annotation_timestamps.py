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


class TestFinderShakeAlertAnnotationTimestamps(unittest.TestCase):
    def setUp(self) -> None:
        config_loader.set_config_root_override(None)
        parser_config.load_profile.cache_clear()
        os.environ.pop("EEWPW_PARSER_CONFIG_ROOT", None)

    def tearDown(self) -> None:
        config_loader.set_config_root_override(None)
        parser_config.load_profile.cache_clear()
        os.environ.pop("EEWPW_PARSER_CONFIG_ROOT", None)

    def test_time_only_annotation_uses_file_date_context(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_root = Path(td)
            _write_json(
                cfg_root / "annotations.json",
                {
                    "annotations": {
                        "time_vs_magnitude": {
                            "finder/shakealert": {
                                "42": "Processing of the time step is stopping because length has decreased!!",
                            }
                        }
                    }
                },
            )

            log_path = cfg_root / "finder_shakealert.log"
            log_path.write_text(
                "\n".join(
                    [
                        "2025/11/07,22:31:25.1000| INFO | Current Time: 2025/11/07,22:31:25.1000",
                        (
                            "2025/11/07,22:31:25.9000| INFO | "
                            "<event_message timestamp=\"2025-11-07T22:31:25.900Z\" "
                            "category=\"live\" instance=\"finder@test\" orig_sys=\"finder\" version=\"1\">"
                        ),
                        (
                            "2025/11/07,22:31:25.9010| INFO | "
                            "<core_info id=\"123\">"
                            "<mag>5.2</mag><lat>35.1</lat><lon>-117.2</lon><depth>10.0</depth>"
                            "<orig_time>2025-11-07T22:31:20.000Z</orig_time></core_info>"
                            "</event_message>"
                        ),
                        "22:31:26:316| INFO |  Processing of the time step is stopping because length has decreased!!",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            config_loader.set_config_root_override(cfg_root)
            parser_config.load_profile.cache_clear()

            parser = FinderParser({"dialect": "shakealert", "verbose": False})
            doc = parser.parse([str(log_path)])

            self.assertGreaterEqual(len(doc.detections), 1)
            anns = doc.annotations["time_vs_magnitude"]
            self.assertGreaterEqual(len(anns), 1)

            ann = next(a for a in anns if a.pattern_id == "finder/shakealert:42")
            self.assertEqual(
                ann.text,
                "22:31:26:316| INFO |  Processing of the time step is stopping because length has decreased!!",
            )
            self.assertEqual(ann.timestamp, "2025-11-07T22:31:26.316000Z")


if __name__ == "__main__":
    unittest.main()
