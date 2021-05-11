from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import sys
import os
from pathlib import Path
from time import sleep
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

state_abbreviation = 'NM'
database_table_name = 'us_nm_legislation'
legislator_table_name = 'us_nm_legislators'

scraper_utils = USStateLegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)

base_url = 'https://www.nmlegis.gov/Legislation/Legislation_List'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)

options = Options()
options.headless = True
driver = webdriver.Chrome(executable_path='H:\Projects\IOTO\goverlytics-scrapers\web_drivers\chrome_win_90.0.4430.24\chromedriver.exe', options=options)
driver.get(base_url)
driver.maximize_window()


def make_soup(url):
    """
    Takes URL and returns soup object.

    :param url: string representing url paths
    :return: soup object
    """

    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'lxml')
    return soup


def get_urls():
    """
    Grab URLS from legislation list.

    :return: a list of URLs
    """

    urls = []

    button = driver.find_element_by_id('MainContent_btnSearch')
    button.click()
    table = driver.find_element_by_css_selector('#MainContent_gridViewLegislation > tbody').find_elements_by_tag_name('tr')
    sleep(2)

    pbar = tqdm(table[0:10])
    for row in pbar:
        link = row.find_element_by_tag_name('a').get_attribute('href')
        pbar.set_description(f'Scraping {link}')
        urls.append(link)
        scraper_utils.crawl_delay(crawl_delay)

    return urls


def open_bill_link(url):
    """
    Opens the link to the bill url.

    :param url: URL to bill
    :return: new selenium driver object
    """

    bill = driver.get(url)
    driver.maximize_window()

    return bill


def get_bill_name(url, row):
    """
    Grab bill name and set it to row.

    :param url: Bill URL
    :param row: Bill row
    """

    soup = make_soup(url)
    name = soup.find('span', {'id': 'MainContent_formViewLegislation_lblTitle'}).text
    row.bill_name = name


def get_bill_title(url, row):
    """
    Grab bill title and set it to row.

    :param url: Bill URL
    :param row: Bill row
    """

    soup = make_soup(url)
    title = soup.find('span', {'id': 'MainContent_formViewLegislation_lblTitle'}).text
    row.bill_title = title


def get_bill_sponsor_info(url, row):
    """
    Grab bill sponsor information(names, ID)

    :param url: Bill URL
    :param row: Bill row
    """

    sponsors = []
    sponsor_ids = []
    soup = make_soup(url)
    table = soup.find('table', {'id': 'MainContent_formViewLegislation'}).find_all('a')
    for name in range(len(table) - 1):
        name = table[name].text
        hn = HumanName(name)
        sponsors.append(f'{hn.last}, {hn.first}')
        sponsor_id = scraper_utils.get_legislator_id(name_last=hn.last, name_first=hn.first)
        sponsor_ids.append(sponsor_id)

    row.sponsors_id = sponsor_ids
    row.sponsors = sponsors

    pprint(sponsor_ids)
    pprint(sponsors)


def get_session(url, row):
    """
    Grab bill session and set it to row.

    :param url: Bill URL
    :param row: Bill row
    """

    soup = make_soup(url)
    header = soup.find('span', {'id': 'MainContent_formViewLegislationTitle_lblSession'}).text
    session = header.split('-')[0]
    row.session = session
    pprint(session)


def scrape(url):
    row = scraper_utils.initialize_row()
    row.source_url = url

    bill_link = open_bill_link(url)
    get_bill_title(url, row)
    get_bill_name(url, row)
    get_bill_sponsor_info(url, row)
    get_session(url, row)



def main():
    urls = get_urls()
    # pprint(urls)

    with Pool() as pool:
        data = pool.map(scrape, urls)


if __name__ == '__main__':
    main()
