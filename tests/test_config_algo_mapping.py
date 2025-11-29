import unittest

from eewpw_parser.config import config_filename_for_algo


class TestConfigAlgoMapping(unittest.TestCase):
    def test_finder_mapping(self):
        self.assertEqual(config_filename_for_algo("finder"), "finder.json")

    def test_vs_mapping(self):
        self.assertEqual(config_filename_for_algo("vs"), "vs.json")

    def test_unknown_algo_raises(self):
        with self.assertRaises(ValueError):
            config_filename_for_algo("unknown")


if __name__ == "__main__":
    unittest.main()
