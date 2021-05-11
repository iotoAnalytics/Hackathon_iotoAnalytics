import sys
import os
from pathlib import Path
from time import sleep
import re
from enum import Enum

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import io
from scraper_utils import CAProvinceTerrLegislationScraperUtils
import requests
from multiprocessing import Pool

from urllib.request import urlopen as uReq

from bs4 import BeautifulSoup as soup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

'''
Change the current legislature to the most recent
'''
CURRENT_LEGISLATURE = 34
PROV_TERR_ABBREVIATION = 'YT'
DATABASE_TABLE_NAME = 'ca_yt'
LEGISLATOR_TABLE_NAME = 'ca_yt_legislators'
BASE_URL = 'https://yukonassembly.ca/'
BILLS_URL = 'https://yukonassembly.ca/house-business/progress-bills'

header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
options = Options()
options.headless = False

driver = webdriver.Chrome('web_drivers/chrome_win_90.0.4430.24/chromedriver.exe', options=options)
driver.switch_to.default_content()

scraper_utils = CAProvinceTerrLegislationScraperUtils(PROV_TERR_ABBREVIATION,
                                                      DATABASE_TABLE_NAME,
                                                      LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

class Main_Functions:
    def program_driver(self):
        self.__open_url()
        current_session = self.__get_current_session()
        SessionScraper(current_session)
        self.__end_driver()

    def __open_url(self):
        driver.get(BILLS_URL)
        driver.maximize_window()
        sleep(2) 

    def __get_current_session(self):
        dropdown_menu = driver.find_element_by_tag_name('select')
        return self.__find_current_sessions(dropdown_menu)

    def __find_current_sessions(self, dropdown_menu):
        dropdown_options = dropdown_menu.find_elements_by_tag_name('option')
        for legislature in dropdown_options:
            if self.__is_current_legislature(legislature):
                return legislature

    def __is_current_legislature(self, legislature):
        return str(CURRENT_LEGISLATURE) in legislature.text

    def __end_driver(self):
        driver.close()
        driver.quit()

class SessionScraper:
    class TableRow(Enum):
        Name = 0
        Title = 1
        Sponsor = 3
        First_Reading = 4
        Second_Reading = 5
        Reported_By_Committee = 6
        Third_Reading = 7
        Assent = 8

    def __init__(self, session):
        session.click()
        self.current_session = self.__get_current_session_as_num(session)
        self.__scrape()
    
    def __get_current_session_as_num(self, session):
        session_as_text = session.text.split(' - ')[1]
        return self.__check_session(session_as_text)

    def __check_session(self, session_text):
        if 'Third' in session_text:
            return 3
        if'Second' in session_text:
            return 2
        if 'First' in session_text:
            return 1

    def __scrape(self):
        active_page_content = self.__get_active_page()
        bill_rows = self.__get_relevant_table_rows(active_page_content)
        bill_data = []
        for bill_row in bill_rows:
            bill_data.append(self.__get_row_data(bill_row))
        print(bill_data)
        
    def __get_active_page(self):
        main_content = driver.find_element_by_css_selector('body > div > div > div > section > div.region.region-content > article > div > div.paragraph.paragraph--type--tabbed-content.paragraph--view-mode--default > div > div.tabs--content')
        main_content_direct_child_divs = main_content.find_elements_by_xpath('./*')
        for div in main_content_direct_child_divs:
            if self.__is_not_hidden_div(div):
                return div

    def __is_not_hidden_div(self, div):
        return 'hidden' not in div.get_attribute('class')

    def __get_relevant_table_rows(self, active_page_content):
        all_tables = active_page_content.find_elements_by_tag_name('table')
        rows = []
        for table in all_tables:
            self.__get_rows(table, rows)
        return rows

    def __get_rows(self, table, rows):
        all_trs = table.find_elements_by_tag_name('tr')[2:]
        for tr in all_trs:
            if len(tr.find_elements_by_xpath('./*')) != 1:
                rows.append(tr)

    def __get_row_data(self, bill_row):
        row = scraper_utils.initialize_row()
        row.source_id = self.__get_source_id(bill_row)
        row.bill_name =self.__get_bill_name(bill_row)
        row.session = str(CURRENT_LEGISLATURE) + "-" + str(self.current_session)
        # All bills are on one page.
        row.source_url = BILLS_URL
        row.chamber_origin = "Legislative Assembly"
        return row
    
    def __get_source_id(self, bill_row):
        bill_pdf_link = self.__get_pdf_link(bill_row)
        return re.findall(r'[0-9]{2}-[0-9]-[A-Za-z]{4}[0-9]{3}', bill_pdf_link)[0]

    def __get_pdf_link(self, bill_row):
        a_tag = bill_row.find_element_by_tag_name('a')
        return a_tag.get_attribute('href')

    def __get_bill_name(self, bill_row):
        return "Bill-" + bill_row.find_elements_by_tag_name('td')[SessionScraper.TableRow.Name.value].text


if __name__ == '__main__':
    main_program = Main_Functions()
    main_program.program_driver()