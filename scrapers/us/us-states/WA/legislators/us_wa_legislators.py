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
ALL_MEMBER_COUNTY_LIST_URL = 'https://app.leg.wa.gov/Rosters/MembersByDistrictAndCounties'

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
    representative_data = MainScraper("Representative").get_data()
    senator_data = MainScraper("Senator").get_data()
    # print(representative_data[:5])
    # print(senator_data[:5])
    


    # # How to match email list to actual person:
    # # find name in the list of names, find position in list.
    # print(len(set(every_email_as_df['Name'].to_list())))

class PreprogramFunctions:
    def __init__(self, url):
        self.driver_instance = SeleniumDriver()
        self.driver_instance.start_driver(url,
                                          state_legislature_crawl_delay)

    def get_emails_as_dataframe(self):
        html = self.driver_instance.get_html_source()
        data_table = self.__extract_emails_table_as_df(html)
        return data_table[['Name', 'Email', 'District']]

    def __extract_emails_table_as_df(self, html):
        html_soup = soup(html, 'html.parser')
        html_email_table = html_soup.find('table', {'id' : 'membertable'})
        table = pd.read_html(str(html_email_table))
        return table[0]

    def get_county_as_dataframe(self):
        html = self.driver_instance.get_html_source()
        data_table = self.__extract_county_table_as_df(html)
        return data_table[['Member', 'District', 'Counties']]
    
    def __extract_county_table_as_df(self, html):
        html_soup = soup(html, 'html.parser')
        html_email_table = html_soup.find('table', {'id' : 'memberbydistrictandcountytable'})
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

class MainScraper:
    # For each member row
    # Look for div with class memberDetails
    # then look for divs with class col-csm-6 col-md-3 memberColumnPad
    # if the count of divs is 3, then get the first div and find the text of all the anchor tags
    # if it's 2, return current year
    def __init__(self, identity):
        self.identity = identity
        if identity == "Representative":
            url = REPRESENTATIVE_PAGE_URL
        else:
            url = SENATOR_PAGE_URL
        self.driver_instance = SeleniumDriver()
        self.driver_instance.start_driver(url, state_legislature_crawl_delay)
        try:
            self.data = self.__get_member_data()
        except:
            error("Error getting member data.")
        finally:
            self.driver_instance.close_driver()

    def get_data(self):
        '''
        Returns a list of each member data (row)
        '''
        return self.data

    def __get_member_data(self):
        main_div = self.driver_instance.driver.find_element_by_id('memberList')
        members_web_element = main_div.find_elements_by_class_name('memberInformation')
        data = []
        for web_element in members_web_element:
            data.append(self.__set_data(web_element))
        return data

    def __set_data(self, member_web_element):
        row = scraper_utils.initialize_row()
        self.__set_name_data(row, member_web_element)
        self.__set_role(row)
        self.__set_party_data(row, member_web_element)
        self.__set_district_and_county(row)
        self.__set_contact_info(row, member_web_element)
        self.__set_years_active(row, member_web_element)
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
        text = text.split(self.identity)[1]
        text = text.split('(')[0]
        return text.strip()

    def __set_role(self, row):
        row.role = self.identity

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

    def __set_district_and_county(self, row):
        name_to_look_for = row.name_last + ', ' + row.name_first
        row.district = self.__set_district(name_to_look_for)
        row.areas_served = self.__set_county(name_to_look_for)

    def __set_district(self, name):
        data_row = every_county_as_df.loc[every_county_as_df['Member'].str.contains(name)]
        district = data_row['District'].values[0]
        district = district.split()[1].strip()
        return int(district)

    def __set_county(self, name):
        data_row = every_county_as_df.loc[every_county_as_df['Member'].str.contains(name)]
        counties = data_row['Counties'].values[0]
        counties = counties.split('Counties ')[1]
        counties = counties.split(', ')
        return counties

    def __set_contact_info(self, row, web_element):
        member_information_div = web_element.find_element_by_css_selector("div[class='row clearfix']")
        member_main_columns = member_information_div.find_elements_by_css_selector("div[class='col-csm-6 col-md-3 memberColumnPad']")
        office_columns = member_main_columns[1:-1] # in between the first and last columns
        row.addresses = self.__get_addresses(office_columns)
        row.email = self.__get_email(row)
        row.phone_numbers = self.__get_numbers(office_columns)

    def __get_addresses(self, offices_web_element):
        addresses = []
        for office in offices_web_element:
            addresses.append(self.__extract_address(office))
        return addresses

    def __extract_address(self, office_web_element):
        office_name = office_web_element.find_element_by_class_name('memberColumnTitle').text
        address = office_web_element.find_element_by_tag_name('span').text
        address = self.__format_address(address)
        return {"location": office_name,
                "address": address}
        
    def __format_address(self, address):
        return address.replace('\n', ', ')

    def __get_email(self, row):
        name_to_look_for = row.name_full
        data_row = every_email_as_df.loc[every_email_as_df['Name'].str.contains(name_to_look_for)]
        email = data_row['Email'].values[0]
        return email.split()[1].strip()

    def __get_numbers(self, offices_web_element):
        numbers = []
        for office in offices_web_element:
            number = self.__extract_number(office)
            if number:
                numbers.append(number)
        return numbers
    
    def __extract_number(self, office_web_element):
        office_name = office_web_element.find_element_by_class_name('memberColumnTitle').text
        number = self.__find_number_from_text(office_web_element.text)
        if number:
            return {"office": office_name,
                    "number": number}
    
    def __find_number_from_text(self, text):
        try:
            number = re.findall(r'\([0-9]{3}\) [0-9]{3}-[0-9]{4}', text)[0]
        except:
            return
        return self.__format_number(number)

    def __format_number(self, number):
        return number.replace('(', '').replace(')', '').replace(' ', '-').strip()


#global variable
every_email_as_df = PreprogramFunctions(ALL_MEMBER_EMAIL_LIST_URL).get_emails_as_dataframe()
every_county_as_df = PreprogramFunctions(ALL_MEMBER_COUNTY_LIST_URL).get_county_as_dataframe()

if __name__ == '__main__':
    program_driver()