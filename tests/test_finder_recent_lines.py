import sys
import unittest

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.finder.dialects import SCFinderDialect, FinderStreamState, FINDER_RECENT_LINES_MAX


class TestFinderRecentLines(unittest.TestCase):
    def test_recent_lines_buffer_bounded_and_ordered(self):
        dialect = SCFinderDialect()
        state = FinderStreamState()

        total = 2100
        lines = [f"line {i}\n" for i in range(1, total + 1)]

        dialect.parse_stream(lines, state=state, finalize=True)

        self.assertTrue(hasattr(state, "recent_lines"))
        self.assertEqual(len(state.recent_lines), FINDER_RECENT_LINES_MAX)

        first_num, first_text = state.recent_lines[0]
        last_num, last_text = state.recent_lines[-1]

        self.assertEqual(last_num, total)
        self.assertEqual(first_num, total - FINDER_RECENT_LINES_MAX + 1)
        self.assertEqual(last_text, lines[-1])


if __name__ == "__main__":
    unittest.main()
