import sys
import os
from pathlib import Path
from time import sleep

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
        current_leglislature_sessions = self.__get_legislature_sessions()
        for session in current_leglislature_sessions:
            SessionScraper(session)
            sleep(2)
        self.__end_driver()

    def __open_url(self):
        driver.get(BILLS_URL)
        driver.maximize_window()
        sleep(2) 

    def __get_legislature_sessions(self):
        dropdown_menu = driver.find_element_by_tag_name('select')
        return self.__find_current_sessions(dropdown_menu)

    def __find_current_sessions(self, dropdown_menu):
        dropdown_options = dropdown_menu.find_elements_by_tag_name('option')
        legislature_session_list = []
        for legislature in dropdown_options:
            if self.__is_current_legislature(legislature):
                legislature_session_list.append(legislature)
        return legislature_session_list

    def __is_current_legislature(self, legislature):
        return str(CURRENT_LEGISLATURE) in legislature.text

    def __end_driver(self):
        driver.close()
        driver.quit()

class SessionScraper:
    def __init__(self, session):
        session.click()
        self.__scrape()
    
    def __scrape(self):
        active_page_content = self.__get_active_page()
        print(len(active_page_content))
        
    def __get_active_page(self):
        main_content = driver.find_element_by_css_selector('body > div > div > div > section > div.region.region-content > article > div > div.paragraph.paragraph--type--tabbed-content.paragraph--view-mode--default > div > div.tabs--content')
        main_content_direct_child_divs = main_content.find_elements_by_xpath('./*')
        for div in main_content_direct_child_divs:
            class_name = div.get_attribute('class')
            if 'hidden' not in class_name:
                return div

if __name__ == '__main__':
    main_program = Main_Functions()
    main_program.program_driver()