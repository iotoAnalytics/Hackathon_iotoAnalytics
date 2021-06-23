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


@scraper_utils.Timer()
def open_driver(url):
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(executable_path=os.path.join('..', '..', '..', '..', '..', 'web_drivers',
                                                           'chrome_win_90.0.4430.24', 'chromedriver.exe'), options=options)
    driver.get(url)
    driver.maximize_window()
    scraper_utils.crawl_delay(crawl_delay)
    return driver


def make_soup(url):
    """
    Takes URL and returns soup object.

    :param url: string representing url paths
    :return: soup object
    """

    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    return soup


@scraper_utils.Timer()
def get_urls(path):
    urls = []
    soup = make_soup(path)
    table = soup.find('table', {'class': 'UItable'}).find_all('tr')[1:]
    pbar = tqdm(table[:20])
    for tr in pbar:
        link = 'https://le.utah.gov/asp/roster/' + tr.find('td').find('a').get('href')
        urls.append(link)
    return urls


@scraper_utils.Timer()
def get_representative_name(url, row):
    driver = open_driver(url)
    full_name = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[1]/div/h1').text.title()
    hn = HumanName(full_name)
    row.name_first = hn.first
    row.name_last = hn.last
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix
    row.name_full = hn.full_name
    # print(hn.full_name)
    driver.quit()


@scraper_utils.Timer()
def get_representative_email(url, row):
    driver = open_driver(url)
    email = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[2]/div/div[3]/div/div[2]/div').text
    row.email = email
    # print(email)
    driver.quit()


@scraper_utils.Timer()
def get_representative_addresses(url, row):
    driver = open_driver(url)
    addresses = []
    location = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[2]/div/div[1]/div/div[2]/h4').text
    address = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[2]/div/div[1]/div/div[2]/div').text
    address_dict = {'address': address, 'location': location}
    addresses.append(address_dict)
    row.addresses = addresses
    # print(addresses)
    driver.quit()


@scraper_utils.Timer()
def get_representative_phone_number(url, row):
    driver = open_driver(url)
    phone_nums = []
    phone = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[2]/div/div[2]/div/div[2]/div').text
    phone_dict = {'phone': phone, 'office': ''}
    phone_nums.append(phone_dict)
    row.addresses = phone_nums
    # print(phone_nums)
    driver.quit()


@scraper_utils.Timer()
def get_representative_party(url, row):
    driver = open_driver(url)
    text = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[1]/div/p[2]').text
    row.party = text.split()[0]
    row.party_id = scraper_utils.get_party_id('Republican' if row.party == 'Republican' else 'Democrat')
    # print(row.party)
    driver.quit()



@scraper_utils.Timer()
def get_representative_role(url, row):
    driver = open_driver(url)
    text = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[1]/div/p[1]').text
    row.role = text.split()[0]
    # print(row.role)
    driver.quit()



@scraper_utils.Timer()
def get_representative_district(url, row):
    driver = open_driver(url)
    text = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[1]/div/p[2]').text
    role = text.split('–')[1].split()
    row.district = " ".join(role[:2])
    # print(row.district)
    driver.quit()


@scraper_utils.Timer()
def get_wiki_info(url, row):
    soup = make_soup(url)
    table = soup.find('table', {'class': 'wikitable sortable'}).find('tbody').find_all('tr')
    for tr in table[1:]:
        name = tr.find_all('td')[1].text.split()
        wiki_first, wiki_last = name[0], name[1]
        if row.name_last == wiki_last and row.name_first[0:2].startswith(wiki_first[0]):
            try:
                path = tr.find_all('td')[1].find('a').get('href')
                wiki_info = scraper_utils.scrape_wiki_bio(WIKI_URL + path)
                row.education = wiki_info['education']
                row.occupation = wiki_info['occupation']
                row.years_active = wiki_info['years_active']
                if wiki_info['birthday'] is not None:
                    row.birthday = str(wiki_info['birthday'])
            except AttributeError:
                pass


def scrape(url):
    row = scraper_utils.initialize_row()
    # soup = make_soup(url)
    if url[-1] == 'H':
        row.source_url = url
        get_representative_name(url, row)
        get_representative_email(url, row)
        get_representative_addresses(url, row)
        get_representative_phone_number(url, row)
        get_representative_role(url, row)
        get_representative_district(url, row)
        get_representative_party(url, row)
        get_wiki_info(WIKI_URL + WIKI_REP_PATH, row)
        # todo get_representative_committees
    print(row)
    return row


def main():
    """
    Map urls to scrape function and write to database..
    """

    urls = get_urls(BASE_URL)

    with Pool() as pool:
        data = pool.map(scrape, urls)

    # scraper_utils.write_data(data, 'us_ut_legislators')


if __name__ == '__main__':
    main()
