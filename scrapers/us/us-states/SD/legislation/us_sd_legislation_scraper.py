import os
import re
import sys

import multiprocessing
from bs4 import BeautifulSoup
from datetime import datetime
from multiprocessing import Pool
from nameparser import HumanName
from pathlib import Path
from pprint import pprint
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import USStateLegislationScraperUtils

DEBUG_MODE = True

STATE_ABBREVIATION = 'SD'
DATABASE_TABLE_NAME = 'us_sd_legislation_test'
LEGISLATOR_TABLE_NAME = 'us_sd_legislators'

BASE_URL = ''
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION, DATABASE_TABLE_NAME, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_urls():
    pass

def scrape(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    # TODO - goverlytics_id
    # TODO - source_id
    # TODO - bill_name
    # TODO - session
    # TODO - date_introduced
    # TODO - source_url    
    # TODO - chamber_origin
    # TODO - committees
    # TODO - bill_type
    # TODO - bill_title
    # TODO - current_status
    # TODO - principal_sponsor_id
    # TODO - principal_sponsor
    # TODO - sponsors
    # TODO - sponsors_id
    # TODO - cosponsors
    # TODO - cosponsors_id
    # TODO - bill_text
    # TODO - bill_description
    # TODO - bill_summary
    # TODO - actions
    # TODO - votes
    # TODO - source_topic

    return row

def _create_soup(url, soup_parser_type):
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def main():
    print('\nSCRAPING SOUTH DAKOTA LEGISLATION\n')

    # Collect legislation urls
    print(DEBUG_MODE and 'Collecting legislation URLs...\n' or '', end='')
    # urls = get_urls()

    # Scrape data from collected URLs
    print(DEBUG_MODE and 'Scraping data from collected URLs...\n' or '', end='')
    # with Pool(NUM_POOL_PROCESSES) as pool:
    #     data = list(tqdm(pool.imap(scrape, urls)))


if __name__ == '__main__':
    main()