from random import sample
import scraper_utils
import unittest

from rows import *

class TestBaseScraperUtils(unittest.TestCase):
    def setUp(self):
        self.sample_sc = scraper_utils.ScraperUtils("ca", "sample_table", CALegislatorRow())
        self.sample_url = "https://thisistest.com/testing/test?yes"

    def testGetBaseURL(self):
        base_url = self.sample_sc.get_base_url(self.sample_url)
        self.assertEqual(self.sample_sc.get_base_url(base_url), "https://thisistest.com")

    # Assuming the add_robots _auto_add_robots work. These require external dependencies 
    
    