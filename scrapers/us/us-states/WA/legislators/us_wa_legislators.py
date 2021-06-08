import sys
import os
from pathlib import Path
import re
import datetime

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from scraper_utils import USStateLegislatorScraperUtils
from urllib.request import urlopen
from bs4 import BeautifulSoup as soup
from multiprocessing import Pool
from nameparser import HumanName
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

WASHINGTON_STATE_LEGISLATURE_BASE_URL = 'https://leg.wa.gov/'
REPRESENTATIVE_PAGE_URL = WASHINGTON_STATE_LEGISLATURE_BASE_URL + 'house/representatives/Pages/default.aspx'
SENATOR_PAGE_URL = WASHINGTON_STATE_LEGISLATURE_BASE_URL + 'Senate/Senators/Pages/default.aspx'
ALL_MEMBER_EMAIL_LIST = 'https://app.leg.wa.gov/MemberEmail/Default.aspx?Chamber=H'

REPUBLICAN_SENATOR_BASE_URL = 'https://src.wastateleg.org/'
REPUBLICAN_SENATOR_PAGE_URL = REPUBLICAN_SENATOR_BASE_URL + 'senators/'
DEMOCRATIC_SENATOR_BASE_URL = 'https://senatedemocrats.wa.gov/'
DEMOCRATIC_SENATOR_PAGE_URL = DEMOCRATIC_SENATOR_BASE_URL + 'senators/'

REPUBLICAN_REPRESENTATIVE_BASE_URL = 'https://houserepublicans.wa.gov/'
REPUBLICAN_REPRESENTATIVE_PAGE_URL = REPUBLICAN_REPRESENTATIVE_BASE_URL + 'representatives/'
DEMOCRATIC_REPRESENTATIVE_BASE_URL = 'http://housedemocrats.wa.gov/'
DEMOCRATIC_REPRESENTATIVE_PAGE_URL = DEMOCRATIC_REPRESENTATIVE_BASE_URL + 'legislators/'

THREADS_FOR_POOL = 12

scraper_utils = USStateLegislatorScraperUtils('WA', 'ca_wa_legislators')
state_legislature_crawl_delay = scraper_utils.get_crawl_delay(WASHINGTON_STATE_LEGISLATURE_BASE_URL)
republican_senator_crawl_delay = scraper_utils.get_crawl_delay(REPUBLICAN_SENATOR_BASE_URL)
democratic_senator_crawl_delay = scraper_utils.get_crawl_delay(DEMOCRATIC_SENATOR_BASE_URL)
republican_representative_crawl_delay = scraper_utils.get_crawl_delay(REPUBLICAN_REPRESENTATIVE_BASE_URL)
democratic_representative_crawl_delay = scraper_utils.get_crawl_delay(DEMOCRATIC_REPRESENTATIVE_BASE_URL)

options = Options()
options.headless = True

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def program_driver():
    pass

if __name__ == '__main__':
    program_driver()