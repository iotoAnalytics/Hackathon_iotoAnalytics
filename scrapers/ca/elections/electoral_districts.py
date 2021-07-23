import sys
import os
from pathlib import Path
from time import sleep
import re
import datetime

NODES_TO_ROOT = 3
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from bs4 import BeautifulSoup as soup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from scraper_utils import ElectoralDistrictScraperUtils
from urllib.request import urlopen

COUNTRY = 'ca'
TABLE = 'ca_electoral_districts'
RIDING_BASE_URL = 'https://lop.parl.ca/'
RIDING_URL = RIDING_BASE_URL + 'sites/ParlInfo/default/en_CA/ElectionsRidings/Ridings'

scraper_utils = ElectoralDistrictScraperUtils(COUNTRY, TABLE)

riding_crawl_delay = scraper_utils.get_crawl_delay(RIDING_URL)

options = Options()
# options.headless = True

def program_driver():
    district_data = Districts().get_data

class Districts:
    def __init__(self):
        self.driver_instance = SeleniumDriver()
        self.driver_instance.start_driver(RIDING_URL, riding_crawl_delay)
        try:
            self.data = self._get_district_data()
        except Exception as e:
            print(e.with_traceback)
            print(f"Error getting riding data")
        self.driver_instance.close_driver()

    def get_data(self):
        return self.data

    def _get_district_data(self) -> list:
        self._display_500_items()
        self._show_inactive_districts()
        sleep(2)
        data_df = self._get_data_as_df()
        print(data_df)

    def _display_500_items(self):
        display_500_items = self.driver_instance.driver.find_element_by_css_selector("div[aria-label='Display 500 items on page']")
        display_500_items.click()
        
    def _show_inactive_districts(self):
        show_inactive_checkbox = self.driver_instance.driver.find_elements_by_class_name('dx-checkbox-icon')[-1]
        show_inactive_checkbox.click()

    def _get_data_as_df(self):
        html = self.driver_instance.get_html_source()
        html_soup = soup(html, 'html.parser')
        html_data_table = html_soup.find('table', {'style':'table-layout: fixed;'})
        return pd.read_html(str(html_data_table))[0]

class SeleniumDriver:
    def __init__(self):
        self.driver = webdriver.Chrome('web_drivers/chrome_win_90.0.4430.24/chromedriver.exe', options=options)
        self.driver.switch_to.default_content()  

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

if __name__ == "__main__":
    program_driver()