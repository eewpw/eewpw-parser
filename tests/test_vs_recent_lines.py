import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.vs.dialects import VSDialect, VSStreamState, VS_RECENT_LINES_MAX


class TestVSRecentLines(unittest.TestCase):
    def test_recent_lines_buffer_bounded_and_ordered(self):
        dialect = VSDialect()
        state = VSStreamState()

        total = 2100
        for i in range(1, total + 1):
            dialect.feed_line(f"dummy line {i}\n", state)

        self.assertTrue(hasattr(state, "recent_lines"))
        self.assertEqual(len(state.recent_lines), VS_RECENT_LINES_MAX)

        first_num, _ = state.recent_lines[0]
        last_num, _ = state.recent_lines[-1]

        self.assertEqual(last_num, total)
        self.assertEqual(first_num, total - VS_RECENT_LINES_MAX + 1)


if __name__ == "__main__":
    unittest.main()
