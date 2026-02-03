import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.eqinfo.eqinfo_parser import EqinfoParser


_REAL_EQINFO_LOG = (
    ROOT / "example-log-files/eqinfo2gm_shakealert/eqInfo2GM_contour_20251113_14.log"
)


def _write_synthetic_eqinfo_log(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "2025/11/13,00:00:00.0000 conlog.2024-04-23 eqInfo2GM_contour Logfile initialized",
        "00:00:00:001|DEBUG | <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>",
        "00:00:00:002|DEBUG | <event_message category=\"live\" instance=\"sa@unit-test\" orig_sys=\"sa\" timestamp=\"2025-11-13T00:00:01.000Z\" version=\"1\">",
        "00:00:00:003|DEBUG | ",
        "00:00:00:004|DEBUG | <core_info id=\"eq-1\">",
        "00:00:00:005|DEBUG | <mag>5.5</mag>",
        "00:00:00:006|DEBUG | <lat>1.0</lat>",
        "00:00:00:007|DEBUG | <lon>2.0</lon>",
        "00:00:00:008|DEBUG | <depth>10.0</depth>",
        "00:00:00:009|DEBUG | <orig_time>2025-11-13T00:00:00.000Z</orig_time>",
        "00:00:00:010|DEBUG | <likelihood>0.90</likelihood>",
        "00:00:00:011|DEBUG | <num_stations>12</num_stations>",
        "00:00:00:012|DEBUG | </core_info>",
        "00:00:00:013|DEBUG | <contributors>",
        "00:00:00:014|DEBUG | <contributor alg_instance=\"epic@unit\" alg_name=\"epic\" alg_version=\"1\" category=\"live\" event_id=\"123\" version=\"0\"/>",
        "00:00:00:015|DEBUG | </contributors>",
        "00:00:00:016|DEBUG | <fault_info>",
        "00:00:00:017|DEBUG | <finite_fault>",
        "00:00:00:018|DEBUG | <vertices>",
        "00:00:00:019|DEBUG | <vertex>",
        "00:00:00:020|DEBUG | <lat>1.1</lat>",
        "00:00:00:021|DEBUG | <lon>2.1</lon>",
        "00:00:00:022|DEBUG | <depth>3.1</depth>",
        "00:00:00:023|DEBUG | </vertex>",
        "00:00:00:024|DEBUG | <vertex>",
        "00:00:00:025|DEBUG | <lat>1.2</lat>",
        "00:00:00:026|DEBUG | <lon>2.2</lon>",
        "00:00:00:027|DEBUG | <depth>3.2</depth>",
        "00:00:00:028|DEBUG | </vertex>",
        "00:00:00:029|DEBUG | </vertices>",
        "00:00:00:030|DEBUG | </finite_fault>",
        "00:00:00:031|DEBUG | </fault_info>",
        "00:00:00:032|DEBUG | <gm_info>",
        "00:00:00:033|DEBUG | <gmpoint_obs>",
        "00:00:00:034|DEBUG | <pga_obs>",
        "00:00:00:035|DEBUG | <obs assoc=\"true\" orig_sys=\"epic\">",
        "00:00:00:036|DEBUG | <SNCL>AAA.BB.CC.--</SNCL>",
        "00:00:00:037|DEBUG | <value>0.1</value>",
        "00:00:00:038|DEBUG | <lat>1.2</lat>",
        "00:00:00:039|DEBUG | <lon>2.2</lon>",
        "00:00:00:040|DEBUG | <time>2025-11-13T00:00:00.100Z</time>",
        "00:00:00:041|DEBUG | </obs>",
        "00:00:00:042|DEBUG | </pga_obs>",
        "00:00:00:043|DEBUG | <pgv_obs>",
        "00:00:00:044|DEBUG | <obs assoc=\"false\" orig_sys=\"epic\">",
        "00:00:00:045|DEBUG | <SNCL>DDD.EE.FF.--</SNCL>",
        "00:00:00:046|DEBUG | <value>0.2</value>",
        "00:00:00:047|DEBUG | <lat>1.3</lat>",
        "00:00:00:048|DEBUG | <lon>2.3</lon>",
        "00:00:00:049|DEBUG | <time>2025-11-13T00:00:00.200Z</time>",
        "00:00:00:050|DEBUG | </obs>",
        "00:00:00:051|DEBUG | </pgv_obs>",
        "00:00:00:052|DEBUG | </gmpoint_obs>",
        "00:00:00:053|DEBUG | </gm_info>",
        "00:00:00:054|DEBUG | </event_message>",
    ]

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_broken_eqinfo_log(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "2025/11/13,00:00:00.0000 conlog.2024-04-23 eqInfo2GM_contour Logfile initialized",
        "00:00:00:001|DEBUG | <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>",
        "00:00:00:002|DEBUG | <event_message category=\"live\" instance=\"sa@unit-test\" orig_sys=\"sa\" timestamp=\"2025-11-13T00:00:01.000Z\" version=\"1\">",
        "00:00:00:003|DEBUG | <core_info id=\"bad-1\">",
        "00:00:00:004|DEBUG | <mag>5.1</mag>",
        "00:00:00:005|DEBUG | <",
        "00:00:00:006|DEBUG | <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>",
        "00:00:00:007|DEBUG | <event_message category=\"live\" instance=\"sa@unit-test\" orig_sys=\"sa\" timestamp=\"2025-11-13T00:00:02.000Z\" version=\"2\">",
        "00:00:00:008|DEBUG | <core_info id=\"good-1\">",
        "00:00:00:009|DEBUG | <mag>5.2</mag>",
        "00:00:00:010|DEBUG | <lat>1.0</lat>",
        "00:00:00:011|DEBUG | <lon>2.0</lon>",
        "00:00:00:012|DEBUG | <depth>10.0</depth>",
        "00:00:00:013|DEBUG | <orig_time>2025-11-13T00:00:00.000Z</orig_time>",
        "00:00:00:014|DEBUG | <likelihood>0.80</likelihood>",
        "00:00:00:015|DEBUG | <num_stations>7</num_stations>",
        "00:00:00:016|DEBUG | </core_info>",
        "00:00:00:017|DEBUG | </event_message>",
    ]

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class TestEqinfoParser(unittest.TestCase):
    def test_parse_synthetic_eqinfo_log(self):
        synthetic_path = ROOT / "tmp" / "synthetic_eqinfo.log"
        _write_synthetic_eqinfo_log(synthetic_path)

        parser = EqinfoParser({"dialect": "shakealert"})
        doc = parser.parse([str(synthetic_path)])

        self.assertEqual(len(doc.detections), 1)
        det = doc.detections[0]

        self.assertIsInstance(det.timestamp, str)
        self.assertIsInstance(det.event_id, str)
        self.assertIsInstance(det.category, str)
        self.assertIsInstance(det.instance, str)
        self.assertIsInstance(det.orig_sys, str)
        self.assertIsInstance(det.version, str)

        self.assertIsInstance(det.core_info.mag, str)
        self.assertIsInstance(det.core_info.lat, str)
        self.assertIsInstance(det.core_info.lon, str)
        self.assertIsInstance(det.core_info.depth, str)
        self.assertIsInstance(det.core_info.orig_time, str)
        self.assertIsInstance(det.core_info.likelihood, str)

        self.assertGreaterEqual(len(det.fault_info), 2)
        for vertex in det.fault_info:
            self.assertIsInstance(vertex.lat, str)
            self.assertIsInstance(vertex.lon, str)
            self.assertIsInstance(vertex.depth, str)

        self.assertIsInstance(det.gm_info.pga_obs, list)
        self.assertIsInstance(det.gm_info.pgv_obs, list)
        self.assertGreaterEqual(len(det.gm_info.pga_obs), 1)
        self.assertGreaterEqual(len(det.gm_info.pgv_obs), 1)

        for obs in det.gm_info.pga_obs + det.gm_info.pgv_obs:
            self.assertIsInstance(obs.SNCL, str)
            self.assertIsInstance(obs.value, str)
            self.assertIsInstance(obs.lat, str)
            self.assertIsInstance(obs.lon, str)
            self.assertIsInstance(obs.time, str)
            self.assertIsInstance(obs.extra, dict)
            for val in obs.extra.values():
                self.assertIsInstance(val, str)

        self.assertIn("contributors", det.extra)
        self.assertIsInstance(det.extra["contributors"], list)
        for contrib in det.extra["contributors"]:
            for value in contrib.values():
                self.assertIsInstance(value, str)

        self.assertIn("num_stations", det.extra)
        self.assertIsInstance(det.extra["num_stations"], str)

    def test_broken_xml_skipped(self):
        broken_path = ROOT / "tmp" / "broken_eqinfo.log"
        _write_broken_eqinfo_log(broken_path)

        parser = EqinfoParser({"dialect": "shakealert"})
        doc = parser.parse([str(broken_path)])

        self.assertEqual(len(doc.detections), 1)
        self.assertEqual(doc.detections[0].event_id, "good-1")

    def test_real_log_no_crash(self):
        parser = EqinfoParser({"dialect": "shakealert"})
        doc = parser.parse([str(_REAL_EQINFO_LOG)])
        self.assertGreaterEqual(len(doc.detections), 1)

    def test_annotations_and_offline_output(self):
        parser = EqinfoParser({"dialect": "shakealert"})
        doc = parser.parse([str(_REAL_EQINFO_LOG)])

        self.assertIn("time_vs_magnitude", doc.annotations)
        ann_list = doc.annotations["time_vs_magnitude"]
        self.assertIsInstance(ann_list, list)

        lines = _REAL_EQINFO_LOG.read_text(encoding="utf-8", errors="ignore").splitlines(True)
        if ann_list:
            for ann in ann_list:
                self.assertIsInstance(ann.timestamp, str)
                self.assertTrue(ann.timestamp.endswith("Z"))
                self.assertIsInstance(ann.line, str)
                self.assertIsInstance(ann.pattern, str)
                self.assertTrue(ann.pattern)
                self.assertTrue(ann.text)
                self.assertIsInstance(ann.text, str)
                self.assertTrue(ann.pattern_id.startswith("eqinfo/shakealert:"))

                line_no = int(ann.line)
                raw_line = lines[line_no - 1].rstrip("\n")
                self.assertEqual(ann.text, raw_line)
        else:
            self.assertEqual(ann_list, [])

        offline_dir = ROOT / "tmp" / "offline_output"
        offline_dir.mkdir(parents=True, exist_ok=True)
        out_path = offline_dir / "eqinfo_20251113_14.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(doc.model_dump(), f, indent=2, ensure_ascii=False)

        self.assertTrue(out_path.exists())
        self.assertGreater(out_path.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
