import unittest
from extract import extract_tools


class SelfTestCase(unittest.TestCase):

    def setUp(self):
        True

    def test_extract(self):
        """Test Extract functions"""

        result = extract_tools.extract_dummy("EXTRACTTEST")
        self.assertEqual(result, "EXTRACTTEST")


if __name__ == '__main__':
    unittest.main()