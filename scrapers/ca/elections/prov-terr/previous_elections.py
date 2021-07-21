import sys
import os
from pathlib import Path
import re
import datetime

NODES_TO_ROOT = 4
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from bs4 import BeautifulSoup as soup
from scraper_utils import PreviousElectionScraperUtils
from urllib.request import urlopen

COUNTRY = 'ca'
TABLE = 'ca_previous_elections'
MAIN_URL = 'https://www.elections.ca/'
PAST_ELECTIONS_URL = MAIN_URL + 'content.aspx?section=ele&dir=pas&document=index&lang=e'

scraper_utils = PreviousElectionScraperUtils(COUNTRY, TABLE)
crawl_delay = scraper_utils.get_crawl_delay(MAIN_URL)

def program_driver():
    general_election_data = GeneralElection().get_election_data()

class MainFunction:
    def get_page_as_soup(self, url):
        page_html = self.__get_site_as_html(url)
        return soup(page_html, 'html.parser')

    def __get_site_as_html(self, url):
        uClient = urlopen(url)
        page_html = uClient.read()
        uClient.close()
        scraper_utils.crawl_delay(crawl_delay)
        return page_html

class GeneralElection:
    def get_election_data(self) -> list:
        page_soup = MainFunction().get_page_as_soup(PAST_ELECTIONS_URL)
        elections = self._get_elections_from_soup(page_soup)
        return self._extract_election_info(elections)

    def _get_elections_from_soup(self, page_soup: soup) -> list:
        main_content = page_soup.find('div', {'id': 'content-main'})
        return main_content.find('ul').find_all('li')

    def _extract_election_info(self, elections: list[soup]) -> list:
        data = []
        for election in elections:
            data.append(self._get_row_data(election))
        return data

    def _get_row_data(self, election: soup):
        row = scraper_utils.initialize_row()
        row.election_id 


if __name__ == '__main__':
    program_driver()