import os
import re
import sys

import multiprocessing
import requests
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
LEGISLATOR_TABLE_NAME = 'us_tn_legislators_test'

BASE_URL = 'https://www.capitol.tn.gov/'
WIKI_URL = 'https://en.wikipedia.org'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)

scraper_utils = USStateLegislatorScraperUtils(STATE_ABBREVIATION, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def main():
    # TODO - source_id
    # TODO - most_recent_term_id
    # TODO - date_collected
    # TODO - source_url
    # TODO - name (full, last, first, middle, suffix)
    # TODO - party_id
    # TODO - party
    # TODO - role
    # TODO - years_active
    # TODO - committees
    # TODO - phone_numbers
    # TODO - addresses
    # TODO - email
    # TODO - birthday
    # TODO - seniority
    # TODO - occupation
    # TODO - education
    # TODO - military_experience
    # TODO - areas_served
    # TODO - district
    pass

if __name__ == '__main__':
    main()