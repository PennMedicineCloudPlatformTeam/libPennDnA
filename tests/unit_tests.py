import unittest
from extract import extract_tools
from load import load_tools
from transform import transform_tools


class SelfTestCase(unittest.TestCase):

    def setUp(self):
        self.extract_dummy = extract_dummy("INPUTSTRING")

    def test_extract(self):
        """Test Extract functions"""

        result = self.extract.extract_dummy("EXTRACTTEST")
        self.assertEqual(result, "EXTRACTTEST")


if __name__ == '__main__':
    unittest.main()