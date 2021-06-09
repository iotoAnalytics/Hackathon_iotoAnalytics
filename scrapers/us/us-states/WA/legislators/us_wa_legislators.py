from logging import error
import sys
import os
from pathlib import Path
import re
import datetime
from time import sleep

from selenium.webdriver.remote import webelement

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
options.headless = True

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def program_driver():
    # every_email_as_df = PreprogramFunctions().get_emails_as_dataframe()
    # print(every_email_as_df)
    representative_data = RepresentativeScraper().get_data()
    print(representative_data)
    


    # # How to match email list to actual person:
    # # find name in the list of names, find position in list.
    # print(len(set(every_email_as_df['Name'].to_list())))

class PreprogramFunctions:
    def __init__(self):
        self.driver_instance = SeleniumDriver()
        self.driver_instance.start_driver(ALL_MEMBER_EMAIL_LIST_URL,
                                          state_legislature_crawl_delay)

    def get_emails_as_dataframe(self):
        html = self.driver_instance.get_html_source()
        return self.__extract_table_as_df(html)

    def __extract_table_as_df(self, html):
        html_soup = soup(html, 'html.parser')
        html_email_table = html_soup.find('table', {'id' : 'membertable'})
        table = pd.read_html(str(html_email_table))
        return table[0]

class SoupMaker:
    def get_page_as_soup(self, url, crawl_delay):
        page_html = self.__get_site_as_html(url, crawl_delay)
        return soup(page_html, 'html.parser')

    def __get_site_as_html(self, url, crawl_delay):
        uClient = urlopen(url)
        page_html = uClient.read()
        uClient.close()
        scraper_utils.crawl_delay(crawl_delay)
        return page_html

class SeleniumDriver:
    def __init__(self):
        self.driver = webdriver.Chrome('web_drivers/chrome_win_90.0.4430.24/chromedriver.exe', options=options)
        self.driver.switch_to.default_content()  

    def start_driver(self, url, crawl_delay):
        try:
            self.driver.get(url)
            self.driver.maximize_window()
        except:
            error("Error opening the website.")
            self.close_driver()
        scraper_utils.crawl_delay(crawl_delay)
        sleep(5)

    def close_driver(self):
        self.driver.close()
        self.driver.quit()

    def get_html_source(self):
        try:
            html = self.driver.page_source
            return html
        except:
            error("Error in getting email table from selenium.")
        finally:
            self.close_driver()

class RepresentativeScraper:
    # For each member row
    # Look for div with class memberDetails
    # then look for divs with class col-csm-6 col-md-3 memberColumnPad
    # if the count of divs is 3, then get the first div and find the text of all the anchor tags
    # if it's 2, return current year
    def __init__(self):
        self.driver_instance = SeleniumDriver()
        self.driver_instance.start_driver(REPRESENTATIVE_PAGE_URL, state_legislature_crawl_delay)
        try:
            self.data = self.__get_representative_data()
        except:
            error("Error getting representative data.")
        finally:
            self.driver_instance.close_driver()

    def get_data(self):
        '''
        Returns a list of each representative data (row)
        '''
        return self.data

    def __get_representative_data(self):
        main_div = self.driver_instance.driver.find_element_by_id('memberList')
        members_web_element = main_div.find_elements_by_class_name('memberInformation')
        data = []
        for web_element in members_web_element:
            data.append(self.__set_data(web_element))
        return data

    def __set_data(self, representative_web_element):
        row = scraper_utils.initialize_row()
        self.__set_name_data(row, representative_web_element)
        self.__set_role(row)
        self.__set_party_data(row, representative_web_element)
        return row

    def __set_name_data(self, row, web_element):
        human_name = self.__get_name(web_element)
        row.name_full = human_name.full_name
        row.name_full = human_name.full_name
        row.name_last = human_name.last
        row.name_first = human_name.first
        row.name_middle = human_name.middle
        row.name_suffix = human_name.suffix

    def __get_name(self, web_element):
        name_container = web_element.find_element_by_class_name('memberName')
        name = self.__extract_name_from_container(name_container)
        return HumanName(name)

    def __extract_name_from_container(self, container):
        text = container.text
        text = text.split('Representative')[1]
        text = text.split('(')[0]
        return text.strip()

    def __set_role(self, row):
        row.role = 'Representative'

    def __set_party_data(self, row, web_element):
        name_container = web_element.find_element_by_class_name('memberName')
        row.party = self.__extract_party_from_container(name_container)
        row.party_id = scraper_utils.get_party_id(row.party)

    def __extract_party_from_container(self, container):
        text = container.text
        text = text.split('(')[1]
        if 'R' in text:
            return 'Republican'
        elif 'D' in text:
            return 'Democrat'

if __name__ == '__main__':
    program_driver()