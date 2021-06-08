import sys
import os
from pathlib import Path
import re
import datetime
from time import sleep

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from scraper_utils import USStateLegislatorScraperUtils
from urllib.request import urlopen
from bs4 import BeautifulSoup as soup
from multiprocessing import Pool
from nameparser import HumanName
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

WASHINGTON_STATE_LEGISLATURE_BASE_URL = 'https://leg.wa.gov/'
REPRESENTATIVE_PAGE_URL = WASHINGTON_STATE_LEGISLATURE_BASE_URL + 'house/representatives/Pages/default.aspx'
SENATOR_PAGE_URL = WASHINGTON_STATE_LEGISLATURE_BASE_URL + 'Senate/Senators/Pages/default.aspx'
ALL_MEMBER_EMAIL_LIST_URL = 'https://app.leg.wa.gov/MemberEmail/Default.aspx?Chamber=H'

REPUBLICAN_SENATOR_BASE_URL = 'https://src.wastateleg.org/'
REPUBLICAN_SENATOR_PAGE_URL = REPUBLICAN_SENATOR_BASE_URL + 'senators/'
DEMOCRATIC_SENATOR_BASE_URL = 'https://senatedemocrats.wa.gov/'
DEMOCRATIC_SENATOR_PAGE_URL = DEMOCRATIC_SENATOR_BASE_URL + 'senators/'

REPUBLICAN_REPRESENTATIVE_BASE_URL = 'https://houserepublicans.wa.gov/'
REPUBLICAN_REPRESENTATIVE_PAGE_URL = REPUBLICAN_REPRESENTATIVE_BASE_URL + 'representatives/'
DEMOCRATIC_REPRESENTATIVE_BASE_URL = 'http://housedemocrats.wa.gov/'
DEMOCRATIC_REPRESENTATIVE_PAGE_URL = DEMOCRATIC_REPRESENTATIVE_BASE_URL + 'legislators/'

THREADS_FOR_POOL = 12

scraper_utils = USStateLegislatorScraperUtils('WA', 'ca_wa_legislators')

# Maybe separate into different classes and use init to initalize these delays
state_legislature_crawl_delay = scraper_utils.get_crawl_delay(WASHINGTON_STATE_LEGISLATURE_BASE_URL)
republican_senator_crawl_delay = scraper_utils.get_crawl_delay(REPUBLICAN_SENATOR_BASE_URL)
democratic_senator_crawl_delay = scraper_utils.get_crawl_delay(DEMOCRATIC_SENATOR_BASE_URL)
republican_representative_crawl_delay = scraper_utils.get_crawl_delay(REPUBLICAN_REPRESENTATIVE_BASE_URL)
democratic_representative_crawl_delay = scraper_utils.get_crawl_delay(DEMOCRATIC_REPRESENTATIVE_BASE_URL)

options = Options()
options.headless = False

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def program_driver():
    every_email_as_df = PreprogramFunctions().get_emails_as_dataframe()

class PreprogramFunctions:
    def get_emails_as_dataframe(self):
        driver_instance = SeleniumDriver()
        driver_instance.start_driver(ALL_MEMBER_EMAIL_LIST_URL,
                                    state_legislature_crawl_delay)
        html = self.__get_html_source(driver_instance)

        return self.__extract_table_as_df(html)
        
    def __get_html_source(self, driver_instance):
        try:
            html = driver_instance.driver.page_source
            return html
        except:
            print("Error in getting email table from selenium.")
        finally:
            driver_instance.close_driver()

    def __extract_table_as_df(self, html):
        html_soup = soup(html, 'html.parser')
        html_email_table = html_soup.find('table', {'id' : 'membertable'})
        table = pd.read_html(str(html_email_table))
        return table[0]

class SeleniumDriver():
    def __init__(self):
        self.driver = webdriver.Chrome('web_drivers/chrome_win_90.0.4430.24/chromedriver.exe', options=options)
        # self.driver.switch_to.default_content()  

    def start_driver(self, url, crawl_delay):
        self.driver.get(url)
        self.driver.maximize_window()
        scraper_utils.crawl_delay(crawl_delay)
        sleep(2)

    def close_driver(self):
        self.driver.close()
        self.driver.quit()

    def get_driver(self):
        return self.driver

class SoupMaker():
    def get_page_as_soup(self, url, crawl_delay):
        page_html = self.__get_site_as_html(url, crawl_delay)
        return soup(page_html, 'html.parser')

    def __get_site_as_html(self, url, crawl_delay):
        uClient = urlopen(url)
        page_html = uClient.read()
        uClient.close()
        scraper_utils.crawl_delay(crawl_delay)
        return page_html

if __name__ == '__main__':
    program_driver()