# region imports
import sys
import os
from pathlib import Path
import pandas as pd
import unittest
from psycopg2 import pool

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[1]

sys.path.insert(0, str(p))

from database import Database
from scraper_utils import ScraperUtils, LegislatorScraperUtils, LegislationScraperUtils, USFedLegislatorScraperUtils, USStateLegislatorScraperUtils, USFedLegislationScraperUtils, USStateLegislationScraperUtils, CAFedLegislatorScraperUtils, CAProvTerrLegislatorScraperUtils, CAFedLegislationScraperUtils, CAProvinceTerrLegislationScraperUtils
from rows import LegislatorRow, LegislationRow, CALegislatorRow, USLegislatorRow, CALegislationRow, USLegislationRow, CAFedLegislatorRow, CAFedLegislationRow
# endregion

class TestScraperUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.utils = ScraperUtils('ca', 'ca_test', LegislatorRow())
    
    def test_database_connection_pool_created(self):
        self.assertIsInstance(Database._connection_pool, pool.SimpleConnectionPool)

    def test_countries_dataframe_created(self):
        self.assertIsInstance(self.__class__.utils.countries, pd.DataFrame)

    def test_parties_dataframe_created(self):
        self.assertIsInstance(self.__class__.utils.parties, pd.DataFrame)

    def test_divisions_dataframe_created(self):
        self.assertIsInstance(self.__class__.utils.divisions, pd.DataFrame)

    def test_country_found(self):
        self.assertIsInstance(self.__class__.utils.country, str)

    def test_country_id_found(self):
        self.assertIsInstance(self.__class__.utils.country_id, int)

    def test_has_custom_request_headers(self):
        self.assertEqual(self.__class__.utils._request_headers, {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/79.0.3945.88 Safari/537.36; IOTO International Inc./enquiries@ioto.ca'
        })

    def test_get_base_url_contains_https_url_with_https(self):
        test_url = 'https://www.test.com/'
        self.assertIn('https', self.__class__.utils.get_base_url(test_url))

    def test_get_base_url_contains_https_url_without_https(self):
        test_url = 'test.com/'
        self.assertIn('https', self.__class__.utils.get_base_url(test_url), f'Test failed using {test_url}')
        test_url = 'www.test.com/'
        self.assertIn('https', self.__class__.utils.get_base_url(test_url), f'Test failed using {test_url}')

    def test_add_robot(self):
        url = 'https://test.com'
        self.__class__.utils.add_robot(url)
        self.assertIsInstance(self.__class__.utils._robots[url], self.__class__.utils.Robots)

    # def test_five_equals_three(self):
    #     self.assertEqual(5, 3)

    # def test_six_equals_one(self):
    #     self.assertEqual(6, 1)

    # def test_on_liberal_partyid_equals_five(self):
    #     self.assertEqual(self.__class__.su.get_party_id('Liberal', 'ON'), 77)


# class TestTwo(unittest.TestCase):
#     def setUp(self):
#         self.su = CAFedLegislatorScraperUtils()
    
#     def test_five_equals_four(self):
#         self.assertEqual(5, 4)

#     def test_bc_liberal_partyid_equals_thirtytwo(self):
#         self.assertEqual(self.su.get_party_id('Liberal', 'BC'), 35)


if __name__ == '__main__':
    unittest.main()



# class TestTwo(unittest.TestCase):
#     su = CAFedLegislatorScraperUtils()
    
#     def test_func(self):
#         assert 10 == 10

#     def test_su(self):
#         assert self.su.get_party_id('Liberal', 'ON') == 5
# class TestTwo(unittest.TestCase):
# def test_fun():
#     assert 5 == 10