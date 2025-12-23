import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.vs.vs_parser import VSParser


SAMPLE_LOG = ROOT / "tests/test-data/scvsmag-processing-info.log"


class TestAnnotationPatternIdNamespace(unittest.TestCase):
    def test_vs_annotations_are_namespaced(self):
        parser = VSParser({"dialect": "scvsmag"})
        doc = parser.parse([str(SAMPLE_LOG)])

        anns = doc.annotations["time_vs_magnitude"]

        self.assertGreater(len(anns), 0)
        for ann in anns:
            self.assertIsNotNone(ann.pattern_id)
            self.assertRegex(ann.pattern_id, r"^[a-z0-9_-]+/[a-z0-9_-]+:[^:]+$")
        self.assertTrue(
            any(pid.split(":", 1)[1] == "start_event" for pid in (a.pattern_id for a in anns))
        )


if __name__ == "__main__":
    unittest.main()
