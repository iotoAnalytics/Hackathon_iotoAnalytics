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

BASE_URL = 'https://legislature.vermont.gov/'
REP_PATH = 'people/all/2022/House'
SENATE_PATH = 'people/all/2022/Senate'

WIKI_URL = 'https://en.wikipedia.org'
WIKI_REP_PATH = '/wiki/Vermont_House_of_Representatives'
WIKI_SENATE_PATH = '/wiki/Vermont_Senate'

scraper_utils = USStateLegislatorScraperUtils('VT', 'us_vt_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)


def timer(func):
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        pprint(f'Finished {func.__name__} in {run_time:.4f} secs')
        return value
    return wrapper_timer


@timer
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


@timer
def open_driver():
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(executable_path=os.path.join('..', '..', '..', '..', '..', 'web_drivers',
                                                           'chrome_win_90.0.4430.24', 'chromedriver.exe'), options=options)
    driver.get(BASE_URL)
    driver.maximize_window()
    return driver


@timer
def get_urls(path):
    """
    Takes base URL of gov site, combine with senate OR house paths to get individual representative page URLS.
    :param path: the rest of the URL path (senator or representative listing)
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


@timer
def get_role(url, row):
    """
    Find legislator role and set row value.

    :param url: url of legislator
    :param row: row of legislator
    """

    soup = make_soup(url)
    header = soup.find('h1')
    role = header.text.split(' ')[0]
    row.role = role


@timer
def get_name(url, row):
    """
    Find legislator name and set row values for first, middle, suffix, last name.

    :param url: url of legislator
    :param row: row of legislator
    """

    soup = make_soup(url)
    header = soup.find('h1')
    info = header.text.split()
    name = " ".join(info[1:])
    hn = HumanName(name)
    row.name_first = hn.first
    if row.name_first == '':
        row.name_first = name.split()[0]
    row.name_last = hn.last
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix
    row.name_full = hn.full_name


@timer
def get_email(url, row):
    """
    Find legislator email and set row value.

    :param url: url of legislator
    :param row: row of legislator
    """

    soup = make_soup(url)
    summary_table = soup.find('dl', {'class': 'summary-table profile-summary'})
    email = summary_table.find_all('dd')[3].text
    row.email = email


@timer
def get_addresses(url, row):
    """
    Find legislator address and set row value.

    :param url: url of legislator
    :param row: row of legislator
    """

    addresses = []
    soup = make_soup(url)
    summary_table = soup.find('dl', {'class': 'summary-table profile-summary'})
    address = summary_table.find_all('dd')[5].text
    location = summary_table.find_all('dt')[5].text
    address_info = {'address': address, 'location': location}
    addresses.append(address_info)
    row.addresses = addresses


@timer
def get_phone_numbers(url, row):
    """
    Find legislator phone number[s] and set row value.

    :param url: url of legislator
    :param row: row of legislator
    """

    phone_numbers = []
    soup = make_soup(url)
    summary_table = soup.find('dl', {'class': 'summary-table profile-summary'})
    phone_number = summary_table.find_all('dd')[4].text
    phone_info = {'phone_number': phone_number, 'office': ''}
    phone_numbers.append(phone_info)
    row.addresses = phone_numbers


@timer
def get_committees(url, row):
    """
    Find legislator committee info and set row value.

    :param url: url of legislator
    :param row: row of legislator
    """

    all_committee_info = []
    soup = make_soup(url)
    item_list = soup.find('ul', {'class': 'item-list'})
    committees = item_list.find_all('li')
    for li in committees:
        try:
            committee = li.find('a').text.strip()
            full_info = li.text.strip('\n').strip('\t').strip()
            role = full_info.replace(committee, '').strip(', ')
            committee_info = {'role': role, 'committee': committee}
            all_committee_info.append(committee_info)
        except AttributeError:
            committee = li.text.strip()
            committee_info = {'role': '', 'committee': committee}
            all_committee_info.append(committee_info)

    row.committees = all_committee_info


@timer
def get_district(url, row):
    """
    Find legislator district info and set row value.

    :param url: url of legislator
    :param row: row of legislator
    """

    soup = make_soup(url)
    summary_table = soup.find('dl', {'class': 'summary-table profile-summary'})
    district = summary_table.find_all('dd')[0].text.strip('\n')
    row.district = district


@timer
def get_party(url, row):
    """
    Find legislator district info and set row value.

    :param url: url of legislator
    :param row: row of legislator
    """

    soup = make_soup(url)
    summary_table = soup.find('dl', {'class': 'summary-table profile-summary'})
    party = summary_table.find_all('dd')[1].text
    row.party = party


@timer
def get_source_id(url, row):
    """
    Find source id  info and set row value.

    :param url: url of legislator
    :param row: row of legislator
    """

    source_id = url.split('/')[-1]
    row.source_id = source_id


@timer
def get_wiki_info(row):
    """
    Grab auxillary(birthday, education, etc) info from wikipedia.
    :param row: legislator row
    """

    if row.role == 'Representative':
        url = WIKI_URL + WIKI_REP_PATH
        soup = make_soup(url)
        table = soup.find_all('table', {'class': 'wikitable sortable'})[1].find('tbody').find_all('tr')
    else:
        url = WIKI_URL + WIKI_SENATE_PATH
        soup = make_soup(url)
        table = soup.find('table', {'class': 'wikitable sortable'}).find('tbody').find_all('tr')

    for tr in table[1:]:
        name = tr.find('td').text.split()
        wiki_first, wiki_last = name[0], name[1]
        if row.name_last == wiki_last and row.name_first[0].startswith(wiki_first[0]):
            try:
                link = tr.find('td').find('a').get('href')
                wiki_info = scraper_utils.scrape_wiki_bio(WIKI_URL + link)
                row.education = wiki_info['education']
                row.most_recent_term_id = wiki_info['most_recent_term_id']
                row.years_active = wiki_info['years_active']
                if wiki_info['birthday'] is not None:
                    row.birthday = str(wiki_info['birthday'])
            except AttributeError:
                pass


def scrape(url):
    """
    Initialize row and scrape legislator data, setting info to the row.

    :param url: legislator url
    """

    row = scraper_utils.initialize_row()
    row.source_url = url
    get_role(url, row)
    get_name(url, row)
    get_email(url, row)
    get_addresses(url, row)
    get_committees(url, row)
    get_district(url, row)
    get_party(url, row)
    get_source_id(url, row)
    get_wiki_info(row)
    return row


def main():
    """
    Map urls to scrape function and write to database..
    """

    representatives_urls = get_urls(REP_PATH)
    senate_urls = get_urls(SENATE_PATH)
    urls = representatives_urls + senate_urls

    with Pool() as pool:
        data = pool.map(scrape, urls)

    scraper_utils.write_data(data, 'us_vt_legislators')


if __name__ == '__main__':
    main()
