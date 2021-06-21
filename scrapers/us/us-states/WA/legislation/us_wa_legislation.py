import datetime
import sys
import os
from pathlib import Path

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import pandas as pd
# from scraper_utils import USStateLegislationScraperUtils
from urllib.request import urlopen
from multiprocessing import Pool
from langdetect import detect
from bs4 import BeautifulSoup as soup
import requests

STATE_ABBREVIATION = 'WA'
DATABASE_TABLE_NAME = 'us_wa_legislation'
LEGISLATOR_TABLE_NAME = 'us_wa_legislators'

BASE_URL = 'http://wslwebservices.leg.wa.gov/'
REQUEST_URL_FOR_GETTING_BILLS = BASE_URL + 'LegislativeDocumentService.asmx/GetAllDocumentsByClass'

THREADS_FOR_POOL = 12
CURRENT_DAY = datetime.date.today()
CURRENT_YEAR = CURRENT_DAY.year

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION,
#                                                DATABASE_TABLE_NAME,
#                                                LEGISLATOR_TABLE_NAME)
# crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

# page_soup = soup(r.text, 'lxml')
# all_documents = page_soup.findAll('legislativedocument')
# for document in all_documents:
#     print(document.find('name').text)

def program_driver():
    pass

class PreProgramFunction:
    def get_biennium(self, year: int):
        if year % 2 == 0:
            past_year = year - 1
            return str(past_year) + '-' + str(year)[2:]
        else:
            next_year = year + 1
            return str(year) + '-' + str(next_year)[2:]

class MainFunctions:
    def request_page(self, url, params):
        return requests.get(url, params=params)

class AllDocumentsByClass:
    def __init__(self):
        self.params = {
            "biennium": CURRENT_BIENNIUM,
            "documentClass": "Bills"
        }

    def get_relevant_bill_information(self):
        bill_info_as_lxml = self.get_all_bill_information_lxml()
        return [self.__extract_relevant_info(bill_lxml) for bill_lxml in bill_info_as_lxml]

    def __extract_relevant_info(self, bill_lxml):
        name = bill_lxml.find('name')
        lfn = bill_lxml.find('longfriendlyname')
        sfn = bill_lxml.find('shortfriendlyname')
        desc = bill_lxml.find('description')
        htmlurl = bill_lxml.find('htmurl')
        pdfurl = bill_lxml.find('pdfurl')
        billid = bill_lxml.find('billid')
        return {
            'name': name,
            'longfriendlyname': lfn,
            'shortfriendlyname': sfn,
            'description': desc,
            'htmurl': htmlurl,
            'pdfurl': pdfurl,
            'billid': billid 
        }

    def get_all_bill_information_lxml(self):
        params = {
            "biennium": CURRENT_BIENNIUM,
            "documentClass": "Bills"
        }
        request = MainFunctions().request_page(REQUEST_URL_FOR_GETTING_BILLS, params=params)
        page_soup = soup(request.text, 'lxml')
        return page_soup.findAll('legislativedocument')

CURRENT_BIENNIUM = PreProgramFunction().get_biennium(CURRENT_YEAR)

if __name__ == '__main__':
    program_driver()