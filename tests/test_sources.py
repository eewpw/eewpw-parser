import tempfile
import threading
import time
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.sources import ReplayLineSource, TailLineSource


class TestSources(unittest.TestCase):
    def test_replay_line_source_matches_file_readlines(self):
        with tempfile.TemporaryDirectory() as td:
            p1 = Path(td) / "f1.log"
            p2 = Path(td) / "f2.log"
            p1.write_text("a\nb\nc", encoding="utf-8")
            p2.write_text("d\ne\n", encoding="utf-8")

            source = ReplayLineSource([str(p1), str(p2)])
            all_lines = []

            for path, lines_iter in source.iterate_files():
                from_file = Path(path).read_text(encoding="utf-8").splitlines(keepends=True)
                iter_lines = list(lines_iter)
                self.assertEqual(iter_lines, from_file)
                all_lines.extend(from_file)

            self.assertEqual(list(iter(source)), all_lines)

    def test_tail_line_source_basic(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "tail.log"
            with p.open("w", encoding="utf-8") as fh:
                fh.write("line0\n")
                fh.flush()

            lines_out = []

            def writer():
                with p.open("a", encoding="utf-8") as fh:
                    for i in range(1, 4):
                        time.sleep(0.02)
                        fh.write(f"line{i}\n")
                        fh.flush()

            t = threading.Thread(target=writer)
            t.start()

            for line in TailLineSource(str(p), seek_end=False, poll_interval=0.01, max_lines=4):
                lines_out.append(line)

            t.join()
            self.assertEqual(lines_out, ["line0\n", "line1\n", "line2\n", "line3\n"])


if __name__ == "__main__":
    unittest.main()
