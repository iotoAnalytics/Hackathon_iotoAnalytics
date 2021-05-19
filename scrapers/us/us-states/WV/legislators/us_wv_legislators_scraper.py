import sys
import os
from pathlib import Path

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
from multiprocessing import Pool
from pprint import pprint
from nameparser import HumanName
import re
from tqdm import tqdm
import datetime

BASE_URL = 'https://www.wvlegislature.gov'
WIKI_URL = 'https://en.wikipedia.org'
SOUP_PARSER_TYPE = 'lxml'

STATE_ABBREVIATION = 'WV'
LEGISLATOR_TABLE_NAME = 'us_wv_legislators_test'

DEBUG_MODE = False
NUM_POOL_THREADS = 10
CURRENT_YEAR = datetime.datetime.now().year

scraper_utils = USStateLegislatorScraperUtils(STATE_ABBREVIATION, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_urls():
    urls = []

    senate_path = '/Senate1'
    senate_roster_path = '/roster.cfm'

    scrape_url = BASE_URL + senate_path + senate_roster_path
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table_rows = soup.find('table', 'tabborder').find_all('tr', {'valign': 'top'})
    urls = [BASE_URL + senate_path + path.find('td').find('a').get('href')
        for path in table_rows]

    return urls

def scrape(url):
    senate_roster_path = '/Senate1/roster.cfm'
    senate_roster_url = BASE_URL + senate_roster_path

    soup = _create_soup(senate_roster_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    # TODO - source_id
    _set_source_id(row, soup)

    # TODO - most_recent_term_id
    # TODO - source_url
    # TODO - name (full, last, first, middle, suffix)
    # TODO - party_id
    # TODO - party
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

    _set_source_id(row, soup)

    return row

def _create_soup(url, soup_parser_type):
    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_source_id(row, soup):
    pass

def _get_legislator_row(data, name_full, district):
    for row in data:
        if name_full == row.name_full and district == row.district:
            return row
    
    return None
 
def main():
    urls = get_urls()

if __name__ == '__main__':
    main()