import io
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.vs.vs_parser import VSParser


SAMPLE_LOG = ROOT.parent / "test-data/parser_train_data/ELM2020/scvsmag-processing-info.log"


class TestVerboseCLI(unittest.TestCase):
    def _capture_verbose(self, files):
        buf = io.StringIO()
        sys_stdout = sys.stdout
        sys.stdout = buf
        try:
            parser = VSParser({"dialect": "scvs", "verbose": True})
            parser.parse([str(p) for p in files])
        finally:
            sys.stdout = sys_stdout
        return buf.getvalue()

    def test_vs_verbose_single_file(self):
        out = self._capture_verbose([SAMPLE_LOG])
        self.assertIn("==== VS Parse ====", out)
        self.assertIn("file=", out)
        self.assertIn("det=", out)
        self.assertIn("ann=", out)
        self.assertIn("---- Summary ----", out)
        self.assertRegex(out, r"Detections: total=\d+ unique=\d+ removed=\d+")
        self.assertRegex(out, r"Annotations: total=\d+ unique=\d+ removed=\d+")

    def test_vs_verbose_duplicate_inputs_show_removed_counts(self):
        # Baseline counts
        base_doc = VSParser({"dialect": "scvs"}).parse([str(SAMPLE_LOG)])
        base_det = len(base_doc.detections)
        base_ann = len(base_doc.annotations["time_vs_magnitude"])

        out = self._capture_verbose([SAMPLE_LOG, SAMPLE_LOG])
        m_det = re.search(r"Detections: total=(\d+) unique=(\d+) removed=(\d+)", out)
        m_ann = re.search(r"Annotations: total=(\d+) unique=(\d+) removed=(\d+)", out)
        self.assertIsNotNone(m_det)
        self.assertIsNotNone(m_ann)
        total_det, uniq_det, removed_det = map(int, m_det.groups())
        total_ann, uniq_ann, removed_ann = map(int, m_ann.groups())

        self.assertGreater(removed_det, 0)
        self.assertGreater(removed_ann, 0)
        self.assertEqual(uniq_det, base_det)
        self.assertEqual(uniq_ann, base_ann)


if __name__ == "__main__":
    unittest.main()
