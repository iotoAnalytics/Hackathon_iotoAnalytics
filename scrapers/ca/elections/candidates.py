import os
from pathlib import Path
import re
import sys
from time import sleep
import traceback

NODES_TO_ROOT = 3
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from bs4 import BeautifulSoup as soup
import pandas
from scraper_utils import CandidatesScraperUtils
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

COUNTRY = 'ca'
CANDIDATES_BASE_URL = 'https://lop.parl.ca'
CANDIDATES_URL = CANDIDATES_BASE_URL + '/sites/ParlInfo/default/en_CA/ElectionsRidings/Elections'

scraper_utils = CandidatesScraperUtils(COUNTRY)
crawl_delay = scraper_utils.get_crawl_delay(CANDIDATES_BASE_URL)

options = Options()
# options.headless = True

def program_driver():
    print("Collecting data...")
    candidate_table_df = Scraper().get_data()

class Scraper:
    def __init__(self):
        self.driver = SeleniumDriver()
        self.driver.start_driver(CANDIDATES_URL, crawl_delay)

        try:
            self.data = self._get_candidate_df()
        except Exception as e:
            print(e.with_traceback(e.__traceback__))

        # self.driver.close_driver()
        
    def get_data(self):
        return self.data

    def _get_candidate_df(self):
        self._prepare_page_for_collection()
    
    def _prepare_page_for_collection(self):
        self._expand_all_entries()
        self._view_1000_entries()
        self._click_next_page()
        self.driver.driver.find_element_by_class_name('asdad').click()

    def _expand_all_entries(self):
        expand_all_button = self.driver.driver.find_element_by_css_selector('#gridContainer > div > div.dx-datagrid-header-panel > div > div > div.dx-toolbar-after > div:nth-child(2) > div > div > div')
        expand_all_button.click()
        sleep(10)

    def _view_1000_entries(self):
        view_1000_div = self.driver.driver.find_elements_by_class_name('dx-page-size')[-1]
        view_1000_div.click()
        sleep(20)
        self.driver.driver.find_element_by_tag_name('html').send_keys('Keys.END')

    def _click_next_page(self):
        next_page_button = self.driver.driver.find_elements_by_class_name('dx-navigate-button')[-1]
        while "dx-button-disable" not in next_page_button.get_attribute('class'):
            next_page_button.click()
            sleep(5)
            next_page_button = self.driver.driver.find_elements_by_class_name('dx-navigate-button')[-1]

class SeleniumDriver:
    def __init__(self):
        self.driver = webdriver.Chrome('web_drivers/chrome_win_92.0.4515.43/chromedriver.exe', options=options)
        self.driver.switch_to.default_content()
        self.tabs = 0

    def start_driver(self, url, crawl_delay):
        try:
            self.driver.get(url)
            self.driver.maximize_window()
        except:
            self.close_driver()
            raise RuntimeError("could not start webdriver")
        scraper_utils.crawl_delay(crawl_delay)
        sleep(10)

    def close_driver(self):
        self.driver.close()
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