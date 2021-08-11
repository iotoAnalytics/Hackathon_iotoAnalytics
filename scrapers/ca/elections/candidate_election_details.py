import os
from pathlib import Path
import re
import sys
from time import sleep
from typing import Union

NODES_TO_ROOT = 3
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from bs4 import BeautifulSoup as soup
from nameparser import HumanName
from numpy import nan
import pandas as pd
from pandas.core.frame import DataFrame
from scraper_utils import CandidatesElectionDetails
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

COUNTRY = 'ca'
BASE_URL = 'https://lop.parl.ca'

scraper_utils = CandidatesElectionDetails(COUNTRY)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

options = Options()
# options.headless = True

def program_driver():
    print("Collecting data...")
    election_links = Elections().get_election_links()
    print(election_links)

class Elections:
    def get_election_links(self) -> list:
        elections_df = scraper_utils.elections
        links = elections_df.loc[elections_df['id'] > 0]['official_votes_record_url'].values
        return [link for link in links]

if __name__ == '__main__':
    program_driver()