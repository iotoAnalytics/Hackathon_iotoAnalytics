# Unavailable data - 
# Wiki data - 

import os
import re
import sys

import multiprocessing
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from multiprocessing import Pool
from nameparser import HumanName
from pathlib import Path
from pprint import pprint
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils

DEBUG_MODE = False

STATE_ABBREVIATION = 'TN'
LEGISLATOR_TABLE_NAME = 'us_tn_legislators'

BASE_URL = 'https://www.capitol.tn.gov/'
LEGISLATORS_PATH = '/Legislators/Listing/44'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)

scraper_utils = USStateLegislatorScraperUtils(STATE_ABBREVIATION, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_urls():
    pass

def scrape(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    # TODO - source_id
    # TODO - most_recent_term_id
    # TODO - source_url
    # TODO - name (full, last, first, middle, suffix)
    # TODO - party_id & party
    # TODO - role
    # TODO - years_active
    # TODO - committees
    # TODO - phone_number
    # TODO - addresses
    # TODO - email
    # TODO - birthday
    # TODO - seniority
    # TODO - occupation
    # TODO - education
    # TODO - military_experience
    # TODO - areas_served
    # TODO - district

    return row

def _create_soup(url, soup_parser_type):
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def main():
    print('\nSCRAPING SOUTH DAKOTA LEGISLATORS\n')

    # Collect legislators urls
    print(DEBUG_MODE and 'Collecting legislator URLs...\n' or '', end='')
    urls = get_urls()

    # Scrape data from collected URLs
    print(DEBUG_MODE and 'Scraping data from legislator URLs...\n' or '', end='')
    with Pool(NUM_POOL_PROCESSES) as pool:
        data = list(tqdm(pool.imap(scrape, urls)))

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()