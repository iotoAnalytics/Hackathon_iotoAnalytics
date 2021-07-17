import sys
import os
from pathlib import Path
import re
import datetime

NODES_TO_ROOT = 4
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from bs4 import BeautifulSoup as soup
from multiprocessing import Pool
from scraper_utils import CAFedPreviousElectionScraperUtils
from urllib.request import urlopen

BASE_URL = 'https://www.elections.ca/'
PAST_ELECTIONS_URL = BASE_URL + 'content.aspx?section=ele&dir=pas&document=index&lang=e'
PROVINCE_TERRITORIES = ['NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB', 'SK', 'AB', 'BC', 'YT', 'NT', 'NU']
THREADS_FOR_POOL = 12

scraper_utils = CAFedPreviousElectionScraperUtils()
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def program_driver():
    prev_election_links = Preparation().get_election_links()

    data = MainFunction().get_data_from_all_links(prev_election_links[3:4])
    

class Preparation:
    def get_election_links(self) -> list:
        page_soup = MainFunction().get_page_as_soup(PAST_ELECTIONS_URL)
        main_container = page_soup.find('div', {'id': 'content-main'})
        links_container = main_container.find('ul')
        return self._get_links(links_container)

    def _get_links(self, container: soup):
        all_links = container.find_all('li')
        return [li.a['href'] for li in all_links]

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

    def get_data_from_all_links(self, links: list) -> list:
        data = []
        for link in links:
            url = BASE_URL + link

            data.extend(MainScraper(url).get_data())
        return data

class MainScraper(MainFunction):
    def __init__(self, url):
        self.url = url
        self.data = self._set_data()

    def get_data(self) -> list:
        return self.data

    def _set_data(self):
        page_soup = self.get_page_as_soup(self.url)
        main_content_div = page_soup.find('div', {'id': 'content-main'})
        links_to_relative_tables = self._find_relative_tables(main_content_div)
        print(links_to_relative_tables)
        return []

    def _find_relative_tables(self, container: soup):
        url = self._find_official_voting_results(container)
        if url[0] == '/':
            official_voting_results_url = BASE_URL + url[1:]
        else:
            official_voting_results_url = BASE_URL + url
        print(official_voting_results_url)
        return self._find_links_to_tables(official_voting_results_url)        

    def _find_official_voting_results(self, container: soup):
        all_lists = container.find_all('li')
        for li in all_lists:
            if "official voting results" in li.text.lower():
                return li.a['href']

    def _find_links_to_tables(self, url):
        '''
        if there is frame in the page (i.e. year 2004, 2006, 2008, 2011),
        then I need to use selenium (check test.py)
        from there, if there is the option element, then I can find value. if not,
        i can look for input and get the value.
        All I need to do after is append that value number + /table_link 

        If there is no frame, then I can just look for tables:
        get the current url, find the last /, then from there, attach the link
        ''' 
        page_soup = self.get_page_as_soup(url)
        all_a_tag = page_soup.find_all('a')
        return_list = []
        for a in all_a_tag:
            if 'table' in a.text.lower():
                return_list.append(a['href'])
        return return_list

if __name__ == "__main__":
    program_driver()