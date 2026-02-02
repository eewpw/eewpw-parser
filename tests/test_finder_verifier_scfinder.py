import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.finder.finder_parser import FinderParser  # noqa: E402
from eewpw_parser.verifiers.finder_scfinder import (  # noqa: E402
    extract_finder_oracle_from_log,
    verify_finder_scfinder,
)


SAMPLE_LOG = ROOT / "example-log-files" / "finder_scfinder" / "scfinder_Elm2020" / "scfinder.log"


class TestFinderVerifierSCFinder(unittest.TestCase):
    def test_finder_verifier_on_example_log_passes(self):
        if not SAMPLE_LOG.exists():
            self.skipTest(f"missing local log file: {SAMPLE_LOG}")
        parser = FinderParser({"dialect": "scfinder"})
        doc = parser.parse([str(SAMPLE_LOG)])
        oracle = extract_finder_oracle_from_log(str(SAMPLE_LOG))
        verify_finder_scfinder(doc, oracle)


if __name__ == "__main__":
    unittest.main()
