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

    def test_valid_sponsors(self):
        self.ca_legislation_row.sponsors = ["john", "david", "beth"]
        self.assertTrue(all(isinstance(sponsor, str) for sponsor in self.ca_legislation_row.sponsors))

    def test_invalid_sponsors(self):
        with self.assertRaises(TypeError):
            self.ca_legislation_row.sponsors = [1, 2, 3]

    def test_valid_sponsor_ids(self):
        self.ca_legislation_row.sponsor_ids = [1, 2, 3]
        self.assertTrue(all(isinstance(sponsor_id, int) for sponsor_id in self.ca_legislation_row.sponsor_ids))

    def test_invalid_sponsor_ids(self):
        with self.assertRaises(TypeError):
            self.ca_legislation_row.sponsors_id = ["1ad", "2asd", "3asda"]