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
from pprint import pprint

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

BASE_URL = 'https://legislature.vermont.gov/'
REP_PATH = 'people/all/2022/House'
SENATE_PATH = 'people/all/2022/Senate'

WIKI_URL = 'https://en.wikipedia.org/'

scraper_utils = USStateLegislatorScraperUtils('NM', 'nm_sc_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)


def make_soup(url):
    """
    Takes senator and representative paths and returns soup object.

    :param url: string representing url paths
    :return: soup object
    """

    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    return soup


def open_driver():
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(
        executable_path='H:\Projects\IOTO\goverlytics-scrapers\web_drivers\chrome_win_90.0.4430.24\chromedriver.exe',
        options=options)
    driver.get(BASE_URL)
    driver.maximize_window()
    return driver


def get_urls(path):
    """
    Takes base URL of gov site, combine with senate OR house paths to get individual representative page URLS.

    :return: a list of representative source URLS
    """

    urls = []
    driver = open_driver()
    driver.get(BASE_URL + path)
    select = Select(driver.find_element_by_name('people_length'))
    select.select_by_visible_text('All')
    sleep(1)

    table = driver.find_element_by_id('people').find_element_by_tag_name('tbody')
    all_rows = table.find_elements_by_tag_name('tr')

    pbar = tqdm(all_rows)
    for row in pbar:
        pbar.set_description('Grabbing URLs')
        link = row.find_element_by_tag_name('a').get_attribute('href')
        urls.append(link)

    driver.quit()
    return urls


def scrape():
    representatives_urls = get_urls(REP_PATH)
    senate_urls = get_urls(SENATE_PATH)
    all_urls = representatives_urls + senate_urls
    pprint(all_urls)

    # with Pool() as pool:
    #     data = pool.map(scrape, senate_urls)


def main():
    scrape()


if __name__ == '__main__':
    main()
