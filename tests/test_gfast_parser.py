import json
import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.gfast.gfast_parser import GfastParser
from eewpw_parser.parsers.gfast.dialects import GfastShakeAlertDialect


_REAL_GFAST_LOG = ROOT / "example-log-files/gfast_shakealert/gfast_eew_20251114.log"


def _use_synthetic_log() -> bool:
    value = os.environ.get("GFAST_USE_SYNTHETIC_LOG", "true")
    return value.strip().lower() not in {"0", "false", "no"}


def _write_synthetic_gfast_log(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "00:00:00:000| INFO | 2025/11/14,00:00:00.000 Change of Calendar Date",
        "00:00:00:005|DEBUG | unpackTB2: skipped 0 msgs, 3 sncl with data",
        "00:00:00:006|DEBUG | unpackTB2 nRead:3 ntraces:3 nReadPtr:3",
        "00:00:00:007| INFO | == Ending unpackTraceBuf2Messages: [Timing: 0.0001s]",
        "00:00:00:008|DEBUG | setData: currentTime:0.008 - ts2:0.004 = ishift=0",
        "00:00:00:009| INFO | gfast_eew: Checking Activemq for events",
        "00:00:00:010| INFO | == [GFAST t0:0.0] Got new amqMessage:",
        "00:00:00:011| INFO | == Sending xml, [GFAST t0:0.0] evid:1 version:0 pgdXML=[<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>",
        "00:00:00:012| INFO | <event_message alg_vers=\"gfast-1.2.6\" category=\"live\" instance=\"sa@unit-test\" message_type=\"update\" orig_sys=\"sa\" ref_id=\"0\" ref_src=\"\" timestamp=\"2025-11-14T00:00:01.000Z\" version=\"7\">",
        "00:00:00:013| INFO | ",
        "00:00:00:014| INFO | <core_info id=\"1\">",
        "00:00:00:015| INFO | <mag units=\"Mw\">5.1</mag>",
        "00:00:00:016| INFO | <lat units=\"deg\">1.0</lat>",
        "00:00:00:017| INFO | <lon units=\"deg\">2.0</lon>",
        "00:00:00:018| INFO | <depth units=\"km\">10.0</depth>",
        "00:00:00:019| INFO | <orig_time units=\"UTC\">2025-11-14T00:00:00.000Z</orig_time>",
        "00:00:00:020| INFO | <likelihood>1.0000</likelihood>",
        "00:00:00:021| INFO | </core_info>",
        "00:00:00:022| INFO | <contributors>",
        "00:00:00:023| INFO | <contributor agency=\"UNIT\" />",
        "00:00:00:024| INFO | </contributors>",
        "00:00:00:025| INFO | <gm_info>",
        "00:00:00:026| INFO | <gmpoint_obs>",
        "00:00:00:027| INFO | <pga_obs>",
        "00:00:00:028| INFO | <obs>",
        "00:00:00:029| INFO | <SNCL>AAA.BB.CC.--</SNCL>",
        "00:00:00:030| INFO | <value units=\"cm/s/s\">0.1</value>",
        "00:00:00:031| INFO | <lat units=\"deg\">1.1</lat>",
        "00:00:00:032| INFO | <lon units=\"deg\">2.1</lon>",
        "00:00:00:033| INFO | <time units=\"UTC\">2025-11-14T00:00:00.100Z</time>",
        "00:00:00:034| INFO | </obs>",
        "00:00:00:035| INFO | </pga_obs>",
        "00:00:00:036| INFO | </gmpoint_obs>",
        "00:00:00:037| INFO | </gm_info>",
        "00:00:00:038| INFO | </event_message>",
        "00:00:00:039| INFO | Start logging for event 1",
        "00:00:00:040| INFO | likelihood: 0.95",
        "00:00:01:000| INFO | <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>",
        "00:00:01:001| INFO | <event_message alg_vers=\"gfast-1.2.6\" category=\"live\" instance=\"sa@unit-test\" message_type=\"update\" orig_sys=\"sa\" ref_id=\"0\" ref_src=\"\" timestamp=\"2025-11-14T00:00:02.000Z\" version=\"7\">",
        "00:00:01:002| INFO | <core_info id=\"2\">",
        "00:00:01:003| INFO | <mag units=\"Mw\">4.9</mag>",
        "00:00:01:004| INFO | <lat units=\"deg\">1.2</lat>",
        "00:00:01:005| INFO | <lon units=\"deg\">2.2</lon>",
        "00:00:01:006| INFO | <depth units=\"km\">12.0</depth>",
        "00:00:01:007| INFO | <orig_time units=\"UTC\">2025-11-14T00:00:01.000Z</orig_time>",
        "00:00:01:008| INFO | <likelihood>0.9000</likelihood>",
        "00:00:01:009| INFO | </core_info>",
        "00:00:01:010| INFO | <gm_info>",
        "00:00:01:011| INFO | <gmpoint_obs>",
        "00:00:01:012| INFO | <pgv_obs>",
        "00:00:01:013| INFO | <obs>",
        "00:00:01:014| INFO | <SNCL>DDD.EE.FF.--</SNCL>",
        "00:00:01:015| INFO | <value units=\"cm/s\">0.2</value>",
        "00:00:01:016| INFO | <lat units=\"deg\">1.3</lat>",
        "00:00:01:017| INFO | <lon units=\"deg\">2.3</lon>",
        "00:00:01:018| INFO | <time units=\"UTC\">2025-11-14T00:00:01.100Z</time>",
        "00:00:01:019| INFO | </obs>",
        "00:00:01:020| INFO | </pgv_obs>",
        "00:00:01:021| INFO | </gmpoint_obs>",
        "00:00:01:022| INFO | </gm_info>",
        "00:00:01:023| INFO | </event_message>",
        "00:00:02:000| INFO | <event_message alg_vers=\"gfast-1.2.6\" category=\"live\" instance=\"sa@unit-test\" message_type=\"update\" orig_sys=\"sa\" ref_id=\"0\" ref_src=\"\" timestamp=\"2025-11-14T00:00:03.000Z\" version=\"7\">",
        "00:00:02:001| INFO | <core_info id=\"3\">",
        "00:00:02:002| INFO | <mag units=\"Mw\">6.0</mag>",
        "00:00:02:003| INFO | <lat units=\"deg\">1.4</lat>",
        "00:00:02:004| INFO | <lon units=\"deg\">2.4</lon>",
        "00:00:02:005| INFO | <depth units=\"km\">8.0</depth>",
        "00:00:02:006| INFO | <orig_time units=\"UTC\">2025-11-14T00:00:02.000Z</orig_time>",
        "00:00:02:007| INFO | <likelihood>0.8000</likelihood>",
        "00:00:02:008| INFO | </core_info>",
        "00:00:02:009| INFO | <gm_info>",
        "00:00:02:010| INFO | <gmpoint_obs>",
        "00:00:02:011| INFO | <pga_obs>",
        "00:00:02:012| INFO | <obs>",
        "00:00:02:013| INFO | <SNCL>GGG.HH.II.--</SNCL>",
        "00:00:02:014| INFO | <value units=\"cm/s/s\">0.3</value>",
        "00:00:02:015| INFO | <lat units=\"deg\">1.5</lat>",
        "00:00:02:016| INFO | <lon units=\"deg\">2.5</lon>",
        "00:00:02:017| INFO | <time units=\"UTC\">2025-11-14T00:00:02.100Z</time>",
        "00:00:02:018| INFO | <",
        "00:00:02:019| INFO | MTH: call newEvent events.nev=1 xml_status.nev=1",
        "00:00:02:020| INFO | </event_message>",
        "00:00:02:021| INFO | End logging for event 1",
        "00:00:02:022| INFO | == Ending unpackTraceBuf2Messages: [Timing: 0.0001s]",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _get_gfast_log_path() -> Path:
    if not _use_synthetic_log():
        return _REAL_GFAST_LOG
    synthetic_path = ROOT / "tmp" / "synthetic_gfast_eew.log"
    _write_synthetic_gfast_log(synthetic_path)
    return synthetic_path


GFAST_LOG = _get_gfast_log_path()


class TestGfastParser(unittest.TestCase):
    def test_parse_gfast_log(self):
        parser = GfastParser({"dialect": "shakealert"})
        doc = parser.parse([str(GFAST_LOG)])

        self.assertGreater(len(doc.detections), 0)
        for det in doc.detections:
            self.assertIsInstance(det.timestamp, str)
            self.assertIsInstance(det.event_id, str)
            self.assertIsInstance(det.version, str)
            self.assertIsInstance(det.core_info.mag, str)
            self.assertIsInstance(det.core_info.lat, str)
            self.assertIsInstance(det.core_info.lon, str)
            self.assertIsInstance(det.core_info.depth, str)
            self.assertIsInstance(det.core_info.orig_time, str)
            if det.core_info.likelihood is not None:
                self.assertIsInstance(det.core_info.likelihood, str)

            self.assertIsInstance(det.gm_info.pga_obs, list)
            self.assertIsInstance(det.gm_info.pgv_obs, list)

            for obs in det.gm_info.pga_obs + det.gm_info.pgv_obs:
                self.assertIsInstance(obs.SNCL, str)
                self.assertIsInstance(obs.value, str)
                self.assertIsInstance(obs.lat, str)
                self.assertIsInstance(obs.lon, str)
                self.assertIsInstance(obs.time, str)
                if obs.orig_sys is not None:
                    self.assertIsInstance(obs.orig_sys, str)

            self.assertIn("contributors", det.extra)
            self.assertIsInstance(det.extra["contributors"], list)
            for contrib in det.extra["contributors"]:
                for value in contrib.values():
                    self.assertIsInstance(value, str)

    def test_xml_block_extraction_count(self):
        parser = GfastParser({"dialect": "shakealert"})
        doc = parser.parse([str(GFAST_LOG)])

        dialect = GfastShakeAlertDialect()
        lines = GFAST_LOG.read_text(encoding="utf-8", errors="ignore").splitlines(True)

        in_block = False
        buf = []
        count = 0
        for line in lines:
            msg = dialect.normalize_line(line)
            if msg is None:
                continue
            if not in_block:
                if "<?xml" in msg or "<event_message" in msg:
                    in_block = True
                    buf = [msg]
                    if "</event_message>" in msg:
                        count += 1
                        buf = []
                        in_block = False
            else:
                buf.append(msg)
                if "</event_message>" in msg:
                    count += 1
                    buf = []
                    in_block = False

        self.assertEqual(count, len(doc.detections))

    def test_annotations_and_offline_output(self):
        parser = GfastParser({"dialect": "shakealert"})
        doc = parser.parse([str(GFAST_LOG)])

        self.assertIn("time_vs_magnitude", doc.annotations)
        ann_list = doc.annotations["time_vs_magnitude"]
        self.assertIsInstance(ann_list, list)

        lines = GFAST_LOG.read_text(encoding="utf-8", errors="ignore").splitlines(True)
        if ann_list:
            for ann in ann_list:
                self.assertIsInstance(ann.timestamp, str)
                self.assertTrue(ann.timestamp.endswith("Z"))
                self.assertIsInstance(ann.line, str)
                self.assertIsInstance(ann.pattern, str)
                self.assertTrue(ann.pattern)
                self.assertTrue(ann.text)
                self.assertIsInstance(ann.text, str)
                self.assertTrue(ann.pattern_id.startswith("gfast/shakealert:"))

                line_no = int(ann.line)
                raw_line = lines[line_no - 1].rstrip("\n")
                self.assertEqual(ann.text, raw_line)
        else:
            self.assertEqual(ann_list, [])

        offline_dir = ROOT / "tmp" / "offline_output"
        offline_dir.mkdir(parents=True, exist_ok=True)
        out_path = offline_dir / "gfast_eew_20251114.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(doc.model_dump(), f, indent=2, ensure_ascii=False)

        self.assertTrue(out_path.exists())
        self.assertGreater(out_path.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
