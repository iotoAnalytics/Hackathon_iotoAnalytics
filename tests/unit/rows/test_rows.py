import unittest

from rows import *

class TestRows(unittest.TestCase):
    def setUp(self):
        self.ca_legislation_row = CALegislationRow()

    def test_valid_string(self):
        self.ca_legislation_row.goverlytics_id = "test"
        self.assertTrue(isinstance(self.ca_legislation_row.goverlytics_id, str))
        
    def test_invalid_string(self):
        with self.assertRaises(TypeError):
            self.ca_legislation_row.goverlytics_id = 11111

    def test_valid_date_introduced(self):
        self.ca_legislation_row.date_introduced = "2022-11-01"
        self.assertTrue(isinstance(self.ca_legislation_row.date_introduced, str))
    
    def test_invalid_date_introduced(self):
        with self.assertRaises(ValueError):
            self.ca_legislation_row.date_introduced = "11-01-2022"