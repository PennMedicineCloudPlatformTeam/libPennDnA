import unittest
from DACPYLib import extract
from DACPYLib import load
from DACPYLib import transform


class SelfTestCase(unittest.TestCase):

    def setUp(self):
        self.extract_dummy = extract_dummy("INPUTSTRING")

    def test_extract(self):
        """Test Extract functions"""

        result = self.extract.extract_dummy("EXTRACTTEST")
        self.assertEqual(result, "EXTRACTTEST")


if __name__ == '__main__':
    unittest.main()