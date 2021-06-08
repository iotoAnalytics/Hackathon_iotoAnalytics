import os
import re
import sys

import multiprocessing
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool
from nameparser import HumanName
from pathlib import Path
from pprint import pprint
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import USStateLegislationScraperUtils

DEBUG_MODE = False

STATE_ABBREVIATION = 'TN'
DATABASE_TABLE_NAME = 'us_tn_legislation_test'
LEGISLATOR_TABLE_NAME = 'us_wv_legislators_test'

BASE_URL = 'https://www.capitol.tn.gov/'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION, DATABASE_TABLE_NAME, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def main():
    # TODO - goverlytics_id
    # TODO - source_id
    # TODO - bill_name
    # TODO - session
    # TODO - date_introduced
    # TODO - source_url
    # TODO - chamber_origin
    # TODO - cimmittees
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
    pass

if __name__ == '__main__':
    main()