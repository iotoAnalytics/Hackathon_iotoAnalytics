import pytest
from scraper_utils import CAFedLegislatorScraperUtils

class TestOne(unittest.TestCase):
    def __init__(self):
        self.su = CAFedLegislatorScraperUtils()
    
    def test_func(self):
        assert 10 == 10

    def test_su(self):
        assert self.su.get_party_id('Liberal', 'ON') == 5

# class TestTwo(unittest.TestCase):
def test_fun():
    assert 5 == 10