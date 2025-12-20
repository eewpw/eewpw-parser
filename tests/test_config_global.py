import unittest

from eewpw_parser.config import load_global_config


class TestConfigGlobal(unittest.TestCase):
    def test_loads_global_config(self):
        cfg = load_global_config()
        self.assertIn("output", cfg)
        self.assertIsInstance(cfg["output"], dict)


if __name__ == "__main__":
    unittest.main()
