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
HOUSE_PATH = '/House'
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
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    # TODO - source_id
    
    # most_recent_term_id
    _set_most_recent_term_id(row, soup)

    # source_url
    _set_source_url(row, url)

    # TODO - name (full, last, first, middle, suffix)
    _set_name(row, soup)
    
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
    # TODO - areas_served
    # TODO - district

    return row

def _create_soup(url, soup_parser_type):
    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_most_recent_term_id(row, soup):
    most_recent_term_id_str = soup.find('table', {'class': 'tabborder'}).find('tr').find('h1').text
    most_recent_term_id = re.search('[0-9]+', most_recent_term_id).group(0)
    row.most_recent_term_id = most_recent_term_id

def _set_source_url(row, url):
    row.source_url = url

# def _set_name(row, soup):
#     soup.find('div', {'id': 'wrapleftcolr'}).find('hr')
#     human_name = HumanName(name)

#     row.name_first = human_name.first
#     row.name_last = human_name.last
#     row.name_middle = human_name.middle
#     row.name_suffix = human_name.suffix
#     row.name_full = human_name.full_name

def _get_legislator_row(data, name_full, district):
    for row in data:
        if name_full == row.name_full and district == row.district:
            return row
    
    return None
 
def main():
    urls = get_urls()

    # data = [scrape(url) for url in urls[0:1]]
    data = scrape('https://www.wvlegislature.gov/House/lawmaker.cfm?member=Delegate%20Anderson')
    
if __name__ == '__main__':
    main()