import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.vs.vs_parser import VSParser
from eewpw_parser.dedup import (
    deduplicate_detections,
    deduplicate_annotations,
)
from eewpw_parser.schemas import Detection, DetectionCore, Annotation


SAMPLE_LOG = Path(__file__).resolve().parent / "test-data/scvsmag-processing-info.log"


class TestDedup(unittest.TestCase):
    def test_vs_dedup_removes_duplicate_detections_and_annotations(self):
        parser = VSParser({"dialect": "scvs"})
        doc1 = parser.parse([str(SAMPLE_LOG)])
        doc2 = parser.parse([str(SAMPLE_LOG), str(SAMPLE_LOG)])

        self.assertEqual(len(doc2.detections), len(doc1.detections))
        self.assertEqual(
            len(doc2.annotations["processing_info"]),
            len(doc1.annotations["processing_info"]),
        )
        # Ensure a known detection appears once
        ids = [d.event_id for d in doc2.detections]
        self.assertEqual(ids.count("gfz2020uzys"), len(ids))

    def test_helper_detection_exact_equality_and_non_equality(self):
        core1 = DetectionCore(
            id="e1",
            mag="5.0",
            lat="1.0",
            lon="2.0",
            depth="3.0",
            orig_time="2020-01-01T00:00:00Z",
            likelihood="0.9",
        )
        d1 = Detection(
            timestamp="2020-01-01T00:00:01Z",
            event_id="e1",
            category="live",
            instance="vs@test",
            orig_sys="vs",
            version="1",
            core_info=core1,
            fault_info=[],
            gm_info={"pgv_obs": [], "pga_obs": []},
        )
        d2 = Detection(
            timestamp="2020-01-01T00:00:01Z",
            event_id="e1",
            category="live",
            instance="vs@test",
            orig_sys="vs",
            version="1",
            core_info=core1,
            fault_info=[],
            gm_info={"pgv_obs": [], "pga_obs": []},
        )
        core3 = DetectionCore(
            id="e1",
            mag="5.0",
            lat="1.0",
            lon="2.0",
            depth="3.0",
            orig_time="2020-01-01T00:00:00Z",
            likelihood="0.8",  # slight difference
        )
        d3 = Detection(
            timestamp="2020-01-01T00:00:02Z",
            event_id="e1",
            category="live",
            instance="vs@test",
            orig_sys="vs",
            version="2",
            core_info=core3,
            fault_info=[],
            gm_info={"pgv_obs": [], "pga_obs": []},
        )

        out = deduplicate_detections([d1, d2, d3])
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0], d1)
        self.assertEqual(out[1], d3)

    def test_helper_annotation_exact_equality(self):
        a1 = Annotation(
            timestamp="2020-01-01T00:00:00Z",
            pattern="p",
            line="1",
            text="line1",
            pattern_id="pid",
        )
        a2 = Annotation(
            timestamp="2020-01-01T00:00:00Z",
            pattern="p",
            line="1",
            text="line1",
            pattern_id="pid",
        )
        a3 = Annotation(
            timestamp="2020-01-01T00:00:00Z",
            pattern="p",
            line="2",
            text="line2",
            pattern_id="pid",
        )

        out = deduplicate_annotations([a1, a2, a3])
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0], a1)
        self.assertEqual(out[1], a3)


if __name__ == "__main__":
    unittest.main()
