import copy
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.vs.vs_parser import VSParser  # noqa: E402
from eewpw_parser.verifiers.vs_scvsmag import extract_vs_oracle_from_log, verify_vs_scvsmag  # noqa: E402


SAMPLE_LOG = ROOT / "example-data" / "Elm2020" / "scvsmag-processing-info.log"


class TestVSSentinelPreservation(unittest.TestCase):
    def test_sentinel_observation_present_and_verified(self):
        parser = VSParser({"dialect": "scvsmag"})
        doc = parser.parse([str(SAMPLE_LOG)])

        sentinels = [
            obs
            for det in doc.detections
            for obs in det.gm_info.pga_obs + det.gm_info.pgv_obs + det.gm_info.pgd_obs
            if (obs.extra.get("vs") or {}).get("is_sentinel") is True and obs.value == "-1.00e+00"
        ]
        self.assertGreaterEqual(len(sentinels), 1)

        oracle = extract_vs_oracle_from_log(str(SAMPLE_LOG))
        verify_vs_scvsmag(doc, oracle)

    def test_missing_sentinel_fails_verifier(self):
        parser = VSParser({"dialect": "scvsmag"})
        doc = parser.parse([str(SAMPLE_LOG)])
        doc_bad = copy.deepcopy(doc)
        removed = False
        for det in doc_bad.detections:
            for lst in (det.gm_info.pga_obs, det.gm_info.pgv_obs, det.gm_info.pgd_obs):
                for idx, obs in enumerate(list(lst)):
                    if (obs.extra.get("vs") or {}).get("is_sentinel"):
                        lst.pop(idx)
                        removed = True
                        break
                if removed:
                    break
            if removed:
                break

        self.assertTrue(removed, "Expected to remove at least one sentinel observation for the negative test")
        oracle = extract_vs_oracle_from_log(str(SAMPLE_LOG))
        with self.assertRaises(AssertionError) as ctx:
            verify_vs_scvsmag(doc_bad, oracle)
        self.assertTrue("sentinel" in str(ctx.exception) or "missing observation" in str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
