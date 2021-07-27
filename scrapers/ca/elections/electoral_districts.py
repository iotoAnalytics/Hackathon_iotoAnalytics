import sys
import os
from pathlib import Path
from time import sleep
import re
import datetime
from numpy import NaN, nan

from pandas.core.frame import DataFrame

NODES_TO_ROOT = 3
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from bs4 import BeautifulSoup as soup
from multiprocessing import Pool
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from scraper_utils import ElectoralDistrictScraperUtils
from urllib.request import urlopen

COUNTRY = 'ca'
TABLE = 'ca_electoral_districts'
RIDING_BASE_URL = 'https://lop.parl.ca/'
RIDING_URL = RIDING_BASE_URL + 'sites/ParlInfo/default/en_CA/ElectionsRidings/Ridings'
THREADS_FOR_POOL = 12

NAME_CHANGE_URLS = {
    2016: 'https://www.elections.ca/content.aspx?section=res&dir=cir/maps2/chang&document=index&lang=e',
    2004: 'https://www.elections.ca/content.aspx?section=res&dir=cir/list&document=index&lang=e',
    2000: 'https://www12.statcan.gc.ca/fedprofil/eng/FedNameChange_E.cfm'
}

DF_COLUMN_INDEX_KV = {
    "name": 0,
    "province": 1,
    "region": 2,
    "start_date": 3,
    "end_date": 4,
    "currently_active": 5
}

scraper_utils = ElectoralDistrictScraperUtils(COUNTRY, TABLE)

riding_crawl_delay = scraper_utils.get_crawl_delay(RIDING_URL)

options = Options()
options.headless = True

def program_driver():
    district_data = Districts().get_data()

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
        try:
            rows_data = self._get_rows_data(data_df)
        except Exception as e:
            print(e)
        print(len(rows_data))

    def _display_500_items(self):
        display_500_items = self.driver_instance.driver.find_element_by_css_selector("div[aria-label='Display 500 items on page']")
        display_500_items.click()
        
    def _show_inactive_districts(self):
        show_inactive_checkbox = self.driver_instance.driver.find_elements_by_class_name('dx-checkbox-icon')[-1]
        show_inactive_checkbox.click()

    def _get_data_as_df(self) -> DataFrame:
        pages_container = self.driver_instance.driver.find_element_by_class_name('dx-pages')
        pages = pages_container.find_elements_by_class_name('dx-page')
        data = self._collect_data()
        for index in range(1, len(pages)):
            pages[index].click()
            sleep(2)
            pages_container = self.driver_instance.driver.find_element_by_class_name('dx-pages')
            pages = pages_container.find_elements_by_class_name('dx-page')
            data = data.append(self._collect_data())
        return data

    def _collect_data(self):
        html = self.driver_instance.get_html_source()
        html_soup = soup(html, 'html.parser')
        html_data_table = html_soup.find('table', {'style':'table-layout: fixed;'})
        return pd.read_html(str(html_data_table))[0]

    def _get_rows_data(self, df: DataFrame) -> list:
        return_data = []
        for index, row in df.iterrows():
            return_data.append(self._add_row_data(row))
        return return_data

    def _add_row_data(self, data_row):
        row = scraper_utils.initialize_row()
        province = data_row[DF_COLUMN_INDEX_KV.get('province')]
        if pd.notna(province):
            row.province_territory_id = self._get_prov_terr_id(province)
        print(row)
        # return row

    def _get_prov_terr_id(self, province):
        df = scraper_utils.divisions
        value = df.loc[df["division"] == province]['id'].values[0]
        try:
            return int(value)
        except Exception:
            return value

class SeleniumDriver:
    def __init__(self):
        self.driver = webdriver.Chrome('web_drivers/chrome_win_92.0.4515.43/chromedriver.exe', options=options)
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