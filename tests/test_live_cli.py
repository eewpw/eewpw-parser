import sys
import unittest
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.live_engine import LiveEngine
from eewpw_parser.sources import TailLineSource
from eewpw_parser.parsers.vs.dialects import VSDialect


SAMPLE_LOG = Path(__file__).resolve().parent / "test-data/scvsmag-processing-info.log"


class TestParseLive(unittest.TestCase):
    def test_live_engine_processes_log_and_writes_output(self):
        with tempfile.TemporaryDirectory() as td:
            data_root = Path(td) / "data_root"

            source = TailLineSource(
                path=str(SAMPLE_LOG),
                poll_interval=0.01,
                seek_end=False,
                max_lines=None,
                follow=False,
            )
            parser = VSDialect()
            engine = LiveEngine(
                source=source,
                parser=parser,
                data_root=data_root,
                algo="vs",
                dialect="scvsmag",
                instance="vs@test",
                verbose=True,
            )

            engine.run_forever()

            target_dir = data_root / "live" / "raw" / "vs"
            files = list(target_dir.glob("*.jsonl"))
            self.assertGreater(len(files), 0)
            contents = files[0].read_text(encoding="utf-8").strip()
            self.assertGreater(len(contents), 0)


if __name__ == "__main__":
    unittest.main()
