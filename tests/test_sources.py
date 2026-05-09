import tempfile
import threading
import time
import unittest
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.sources import ReplayLineSource, TailLineSource


class TestSources(unittest.TestCase):
    def _wait_for(self, predicate, timeout=2.0, msg="condition not reached"):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(0.005)
        self.fail(msg)

    def _start_collecting(self, source, out_lines, out_errors):
        def reader():
            try:
                for line in source:
                    out_lines.append(line)
            except Exception as exc:
                out_errors.append(exc)

        t = threading.Thread(target=reader, daemon=True)
        t.start()
        return t

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

    def test_tail_line_source_rotation_rename_recreate(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "tail.log"
            rot = Path(td) / "tail.log.1"
            p.write_text("old0\n", encoding="utf-8")

            lines_out = []
            errors = []
            source = TailLineSource(str(p), seek_end=False, poll_interval=0.01, max_lines=3, follow=True)
            t = self._start_collecting(source, lines_out, errors)

            self._wait_for(lambda: len(lines_out) >= 1, msg="initial line not consumed")
            os.rename(p, rot)
            p.write_text("new1\n", encoding="utf-8")
            with p.open("a", encoding="utf-8") as fh:
                fh.write("new2\n")
                fh.flush()

            t.join(timeout=2.0)
            self.assertFalse(t.is_alive(), "tail reader did not finish")
            self.assertEqual(errors, [])
            self.assertEqual(lines_out, ["old0\n", "new1\n", "new2\n"])

    def test_tail_line_source_follows_path_not_old_rotated_inode(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "tail.log"
            rot = Path(td) / "tail.log.1"
            p.write_text("old0\n", encoding="utf-8")

            lines_out = []
            errors = []
            source = TailLineSource(str(p), seek_end=False, poll_interval=0.01, max_lines=3, follow=True)
            t = self._start_collecting(source, lines_out, errors)

            self._wait_for(lambda: len(lines_out) >= 1, msg="initial line not consumed")
            os.rename(p, rot)
            p.write_text("new1\n", encoding="utf-8")
            self._wait_for(lambda: "new1\n" in lines_out, msg="tailer did not switch to recreated path")

            with rot.open("a", encoding="utf-8") as fh:
                fh.write("stale_from_rotated\n")
                fh.flush()
            with p.open("a", encoding="utf-8") as fh:
                fh.write("new2\n")
                fh.flush()

            t.join(timeout=2.0)
            self.assertFalse(t.is_alive(), "tail reader did not finish")
            self.assertEqual(errors, [])
            self.assertEqual(lines_out, ["old0\n", "new1\n", "new2\n"])

    def test_tail_line_source_rotation_missing_path_then_recreate(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "tail.log"
            rot = Path(td) / "tail.log.1"
            p.write_text("old0\n", encoding="utf-8")

            lines_out = []
            errors = []
            source = TailLineSource(str(p), seek_end=False, poll_interval=0.01, max_lines=2, follow=True)
            t = self._start_collecting(source, lines_out, errors)

            self._wait_for(lambda: len(lines_out) >= 1, msg="initial line not consumed")
            os.rename(p, rot)
            time.sleep(0.05)
            p.write_text("new1\n", encoding="utf-8")

            t.join(timeout=2.0)
            self.assertFalse(t.is_alive(), "tail reader did not finish")
            self.assertEqual(errors, [])
            self.assertEqual(lines_out, ["old0\n", "new1\n"])

    def test_tail_line_source_truncate_in_place(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "tail.log"
            p.write_text("line0\n", encoding="utf-8")

            lines_out = []
            errors = []
            source = TailLineSource(str(p), seek_end=False, poll_interval=0.01, max_lines=3, follow=True)
            t = self._start_collecting(source, lines_out, errors)

            self._wait_for(lambda: len(lines_out) >= 1, msg="initial line not consumed")
            with p.open("a", encoding="utf-8") as fh:
                fh.write("line1\n")
                fh.flush()
            self._wait_for(lambda: len(lines_out) >= 2, msg="second line not consumed")

            with p.open("r+", encoding="utf-8") as fh:
                fh.truncate(0)
                fh.seek(0)
                fh.write("line2\n")
                fh.flush()

            t.join(timeout=2.0)
            self.assertFalse(t.is_alive(), "tail reader did not finish")
            self.assertEqual(errors, [])
            self.assertEqual(lines_out, ["line0\n", "line1\n", "line2\n"])

    def test_tail_line_source_seek_end_startup_behavior_preserved(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "tail.log"
            p.write_text("old0\nold1\n", encoding="utf-8")

            lines_out = []
            errors = []
            source = TailLineSource(str(p), seek_end=True, poll_interval=0.01, max_lines=1, follow=True)
            t = self._start_collecting(source, lines_out, errors)

            time.sleep(0.05)
            with p.open("a", encoding="utf-8") as fh:
                fh.write("new0\n")
                fh.flush()

            t.join(timeout=2.0)
            self.assertFalse(t.is_alive(), "tail reader did not finish")
            self.assertEqual(errors, [])
            self.assertEqual(lines_out, ["new0\n"])

    def test_tail_line_source_follow_false_behavior_preserved(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "tail.log"
            p.write_text("line0\nline1\n", encoding="utf-8")

            lines = list(TailLineSource(str(p), seek_end=False, poll_interval=0.01, follow=False))
            self.assertEqual(lines, ["line0\n", "line1\n"])

            lines = list(TailLineSource(str(p), seek_end=True, poll_interval=0.01, follow=False))
            self.assertEqual(lines, [])

    def test_tail_line_source_follow_true_buffers_partial_until_newline(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "tail.log"
            p.write_text("", encoding="utf-8")

            lines_out = []
            errors = []
            source = TailLineSource(str(p), seek_end=False, poll_interval=0.01, max_lines=1, follow=True)
            t = self._start_collecting(source, lines_out, errors)

            with p.open("a", encoding="utf-8") as fh:
                fh.write("part")
                fh.flush()

            time.sleep(0.05)
            self.assertEqual(lines_out, [])

            with p.open("a", encoding="utf-8") as fh:
                fh.write("ial\n")
                fh.flush()

            t.join(timeout=2.0)
            self.assertFalse(t.is_alive(), "tail reader did not finish")
            self.assertEqual(errors, [])
            self.assertEqual(lines_out, ["partial\n"])


if __name__ == "__main__":
    unittest.main()
