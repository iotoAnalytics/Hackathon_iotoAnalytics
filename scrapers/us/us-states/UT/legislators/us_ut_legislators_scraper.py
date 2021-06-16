import sys
import os
from pathlib import Path
from nameparser import HumanName
from bs4 import BeautifulSoup
from scraper_utils import USStateLegislatorScraperUtils
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
import pandas as pd
from multiprocessing import Pool
from time import sleep
import time
from pprint import pprint

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

BASE_URL = 'https://le.utah.gov/asp/roster/roster.asp'

WIKI_URL = 'https://en.wikipedia.org'
WIKI_REP_PATH = '/wiki/Utah_House_of_Representatives'
WIKI_SENATE_PATH = '/wiki/Utah_State_Senate'

scraper_utils = USStateLegislatorScraperUtils('UT', 'us_ut_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)


def open_driver(url):
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(executable_path=os.path.join('..', '..', '..', '..', '..', 'web_drivers',
                                                           'chrome_win_90.0.4430.24', 'chromedriver.exe'), options=options)
    driver.get(url)
    driver.maximize_window()
    return driver


def make_soup(url):
    """
    Takes URL and returns soup object.

    :param url: string representing url paths
    :return: soup object
    """

    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    return soup


scraper_utils.Timer()
def get_urls(path):
    urls = []
    soup = make_soup(path)
    table = soup.find('table', {'class': 'UItable'}).find('tbody').find_all('tr')[1:]
    for tr in table:
        link = tr.find('a').get_attribute('href')
        urls.append(link)

    return urls


scraper_utils.Timer()
def get_name(url, row):
    driver = open_driver(url)
    full_name = driver.find_element_by_class_name('et_pb_text_inner').find_element_by_tag_name('h1')
    hn = HumanName(full_name)
    row.name_first = hn.first
    row.name_last = hn.last
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix
    row.name_full = hn.full_name


scraper_utils.Timer()
def get_email(url, row):
    driver = open_driver(url)
    table = driver.find_element_by_class_name('et_pb_text_inner')
    email_container = table.find_elements_by_tag_name('div')[2].find_element_by_tag_name('div').find_element_by_class_name('et_pb_blurb_container')
    email = email_container.find_element_by_class_name('et_pb_blurb_description').text
    row.email = email


scraper_utils.Timer()
def get_addresses(url, row):
    addresses = []
    driver = open_driver(url)
    table = driver.find_element_by_class_name('et_pb_text_inner')
    address_container = table.find_elements_by_tag_name('div')[0].find_element_by_tag_name('div').find_element_by_class_name('et_pb_blurb_container')
    location = address_container.find_element_by_tag_name('h4')
    address = address_container.find_element_by_tag_name('div')
    address_dict = {'address': address, 'location': location}
    addresses.append(address_dict)
    row.addresses = addresses


scraper_utils.Timer()
def get_phone_number(url, row):
    phone_nums = []
    driver = open_driver(url)
    table = driver.find_element_by_class_name('et_pb_text_inner')
    phone_container = table.find_elements_by_tag_name('div')[1].find_element_by_tag_name('div').find_element_by_class_name('et_pb_blurb_container')
    phone = phone_container.find_element_by_tag_name('div')
    phone_dict = {'phone': phone, 'office': ''}
    phone_nums.append(phone_dict)
    row.addresses = phone_nums


def scrape(url):
    row = scraper_utils.initialize_row()
    soup = make_soup(url)
    get_name(url, row)
    get_email(url, row)
    get_addresses(url, row)
    # todo get_phone_numbers
    # todo get_role
    # todo get_district
    # todo get_party
    # todo get_wiki_info
    # todo get_source_url

    return row


def main():
    """
    Map urls to scrape function and write to database..
    """

    urls = get_urls(BASE_URL)
    print(urls)
    # with Pool() as pool:
    #     data = pool.map(scrape, urls)

    scraper_utils.write_data(data, 'us_ut_legislators')


if __name__ == '__main__':
    main()
