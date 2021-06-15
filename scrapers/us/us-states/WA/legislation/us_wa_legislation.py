import datetime
import sys
import os
from pathlib import Path

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import pandas as pd
from scraper_utils import CAProvinceTerrLegislationScraperUtils, USStateLegislationScraperUtils
from urllib.request import urlopen
from multiprocessing import Pool
from langdetect import detect
from bs4 import BeautifulSoup as soup
import requests


STATE_ABBREVIATION = 'WA'
DATABASE_TABLE_NAME = 'us_wa_legislation'
LEGISLATOR_TABLE_NAME = 'us_wa_legislators'
BASE_URL = 'http://wslwebservices.leg.wa.gov/'
THREADS_FOR_POOL = 12
CURRENT_DAY = datetime.date.today()
CURRENT_YEAR = CURRENT_DAY.year

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION,
                                               DATABASE_TABLE_NAME,
                                               LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)


# can make biennium a variable because this will be constant
params = {
    "biennium": "2021-22",
    "documentClass": "Bills"
}

r = requests.get(BASE_URL + '/LegislativeDocumentService.asmx/GetAllDocumentsByClass', params=params)
page_soup = soup(r.text, 'lxml')
all_documents = page_soup.findAll('legislativedocument')
for document in all_documents:
    print(document.find('name').text)

class SoupMaker:
    def get_page_as_soup(self, url, crawl_delay):
        page_html = self.__get_site_as_html(url, crawl_delay)
        return soup(page_html, 'lxml')

    def __get_site_as_html(self, url, crawl_delay):
        uClient = urlopen(url)
        page_html = uClient.read()
        uClient.close()
        scraper_utils.crawl_delay(crawl_delay)
        return page_html