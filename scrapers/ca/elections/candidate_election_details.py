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
from multiprocessing import Pool
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
    election_id_and_links = DataFrames().get_election_links()
    row_data = get_data_from_all_links(collect_election_data, election_id_and_links[:2])
    print(row_data)

def get_data_from_all_links(function, iterable):
    data = []
    with Pool() as pool:
        data = pool.map(func=function,
                        iterable=iterable)
    return data

def collect_election_data(id_and_link):
    election_data_scraper = Election(id_and_link)
    return election_data_scraper.get_row()

class DataFrames:
    def get_election_links(self) -> list:
        elections_df = scraper_utils.elections
        id_and_links = elections_df.loc[elections_df['id'] > 0][['id', 'official_votes_record_url']].values
        return [(id, link) for id, link in id_and_links]

class Election:
    def __init__(self, id_and_link):
        self.id_and_link = id_and_link
        self.row = scraper_utils.initialize_row()
        self.driver = SeleniumDriver()

        self._collect_data()

    def get_row(self):
        return self.row

    def _collect_data(self):
        self.row.election_id = int(self.id_and_link[0])
        self._election_data_from_links()

    def _election_data_from_links(self):
        link = self.id_and_link[1]
        self.driver.start_driver(link, crawl_delay)
        self._prepare_site_for_collection()

    def _prepare_site_for_collection(self):
        self._expand_all()
        try:
            self._view_1000_items()
        except:
            print(f"No 1000 items button for election_id: {self.row.election_id}")
        
    def _expand_all(self):
        expand_all_checkbox = self.driver.driver.find_element_by_class_name('dx-checkbox-icon')
        expand_all_checkbox.click()
        sleep(3)

    def _view_1000_items(self):
        page_sizes = self.driver.driver.find_elements_by_class_name('dx-page-size')
        page_sizes[-1].click()
        sleep(3)

class SeleniumDriver:
    """
    Used to handle Selenium.
    """
    def __init__(self):
        self.driver = webdriver.Chrome('web_drivers/chrome_win_92.0.4515.43/chromedriver.exe', options=options)
        self.driver.switch_to.default_content()
        self.tabs = 0

    def start_driver(self, url, crawl_delay):
        try:
            self.tabs +=1
            self.driver.get(url)
            self.driver.maximize_window()
        except:
            self.tabs -=1
            self.close_driver()
            raise RuntimeError("could not start webdriver")
        scraper_utils.crawl_delay(crawl_delay)
        sleep(5)

    def close_driver(self):
        self.driver.close()
        self.tabs -=1
        self.driver.quit()

    def get_html_source(self):
        try:
            html = self.driver.page_source
            return html
        except:
            self.close_driver()
            raise RuntimeError("Error in getting email table from selenium.")

if __name__ == '__main__':
    program_driver()