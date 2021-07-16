import sys
import os
from pathlib import Path
import re
import datetime

NODES_TO_ROOT = 4
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from scraper_utils import CAFedPreviousElectionScraperUtils

BASE_URL = 'https://www.elections.ca/'
PAST_ELECTIONS_URL = BASE_URL + 'content.aspx?section=ele&dir=pas&document=index&lang=e'

scraper_utils = CAFedPreviousElectionScraperUtils()
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

