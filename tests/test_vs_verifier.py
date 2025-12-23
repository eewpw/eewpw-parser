import copy
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.vs.vs_parser import VSParser  # noqa: E402
from eewpw_parser.verifiers.vs_scvsmag import extract_vs_oracle_from_log, verify_vs_scvsmag  # noqa: E402


SAMPLE_LOG = ROOT / "example-data" / "Elm2020" / "scvsmag-processing-info.log"


class TestVSVerifier(unittest.TestCase):
    def test_vs_verifier_on_example_log_pass(self):
        parser = VSParser({"dialect": "scvsmag"})
        doc = parser.parse([str(SAMPLE_LOG)])
        oracle = extract_vs_oracle_from_log(str(SAMPLE_LOG))
        verify_vs_scvsmag(doc, oracle)

    def test_vs_verifier_missing_pgv_fails(self):
        parser = VSParser({"dialect": "scvsmag"})
        doc = parser.parse([str(SAMPLE_LOG)])
        doc_bad = copy.deepcopy(doc)
        doc_bad.detections[0].gm_info.pgv_obs.pop(0)
        oracle = extract_vs_oracle_from_log(str(SAMPLE_LOG))
        with self.assertRaises(AssertionError) as ctx:
            verify_vs_scvsmag(doc_bad, oracle)
        self.assertIn("observation", str(ctx.exception))

    def test_vs_verifier_component_mismatch_fails(self):
        parser = VSParser({"dialect": "scvsmag"})
        doc = parser.parse([str(SAMPLE_LOG)])
        doc_bad = copy.deepcopy(doc)
        obs = doc_bad.detections[0].gm_info.pga_obs[0]
        comp = (obs.extra.get("vs") or {}).get("component")
        obs.extra["vs"]["component"] = "H" if comp != "H" else "Z"
        oracle = extract_vs_oracle_from_log(str(SAMPLE_LOG))
        with self.assertRaises(AssertionError) as ctx:
            verify_vs_scvsmag(doc_bad, oracle)
        self.assertIn("observation", str(ctx.exception))

    def test_vs_verifier_timestamp_mismatch_fails(self):
        parser = VSParser({"dialect": "scvsmag"})
        doc = parser.parse([str(SAMPLE_LOG)])
        doc_bad = copy.deepcopy(doc)
        obs = doc_bad.detections[0].gm_info.pga_obs[0]
        obs.time = "1999-01-01T00:00:00Z"
        oracle = extract_vs_oracle_from_log(str(SAMPLE_LOG))
        with self.assertRaises(AssertionError) as ctx:
            verify_vs_scvsmag(doc_bad, oracle)
        self.assertIn("time mismatch", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
