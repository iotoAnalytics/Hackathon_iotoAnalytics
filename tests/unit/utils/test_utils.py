import json
import unittest

from datetime import datetime
from utils import *

class TestUtils(unittest.TestCase):

    def test_dot_dict(self):
        sample_dict = {}

        sample_dict = DotDict(sample_dict)
        sample_dict.test = True

        self.assertEqual(sample_dict["test"], True)
        self.assertEqual(sample_dict.test, True)

    def test_json_serial(self):
        date = datetime.strptime("July 14, 1995", "%B %d, %Y")
        sample_obj = {"date": date}
        self.assertTrue(isinstance(json.dumps(sample_obj, default=json_serial), str))