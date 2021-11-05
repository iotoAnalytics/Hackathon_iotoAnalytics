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

    def test_invalid_source_url(self):
        with self.assertRaises(ValueError):
            self.ca_legislation_row.source_url = ""

    def test_valid_committees(self):
        self.ca_legislation_row.committees = [{"chamber":"x", "committee":"y"}, {"chamber":"a", "committee":"b"}]
        self.assertTrue(isinstance(self.ca_legislation_row.committees, list))
        self.assertTrue(all(isinstance(committee, dict) for committee in self.ca_legislation_row.committees))

    def test_invalid_committees(self):
        with self.assertRaises(TypeError):
            self.ca_legislation_row.committees = [[], []]

        with self.assertRaises(ValueError):
            self.ca_legislation_row.committees = [{"committee":"y"}]

        with self.assertRaises(ValueError):
            self.ca_legislation_row.committees = [{"chamber":"x"}]
    
        with self.assertRaises(ValueError):
            self.ca_legislation_row.committees = [{"chamber":[], "committee":[]}]

    def test_valid_int(self):
        self.ca_legislation_row.principal_sponsor_id = 1111
        self.assertTrue(isinstance(self.ca_legislation_row.principal_sponsor_id, int))\

    def test_invalid_int(self):
        with self.assertRaises(TypeError):
            self.ca_legislation_row.principal_sponsor_id = "1111"

    def test_valid_principal_sponsor(self):
        self.ca_legislation_row.principal_sponsor = "John Smith"
        self.assertTrue(isinstance(self.ca_legislation_row.principal_sponsor, str))

    def test_invalid_principal_sponsor(self):
        with self.assertRaises(ValueError):
            self.ca_legislation_row.principal_sponsor = ""