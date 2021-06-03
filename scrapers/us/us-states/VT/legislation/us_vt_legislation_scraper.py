import sys
import os
from pathlib import Path
from nameparser import HumanName
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
from multiprocessing import Pool
from time import sleep
import time
from pprint import pprint
from tika import parser

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import USStateLegislationScraperUtils

BASE_URL = 'https://legislature.vermont.gov'
BILL_PATH = '/bill/introduced/2022'
RESOLUTIONS_PATH = '/bill/resolutions-introduced/2022'

state_abbreviation = 'VT'
database_table_name = 'us_vt_legislation'
legislator_table_name = 'us_vt_legislators'

scraper_utils = USStateLegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)
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


@scraper_utils.Timer()
def get_urls(path):
    """
    Grabs urls. Bills and resolutions are on different pages so tries both.
    :param path: resolution or bill path
    :return: list of urls
    """

    urls = []
    driver = open_driver(BASE_URL)
    driver.get(BASE_URL + path)

    try:
        select = Select(driver.find_element_by_name('bills-introduced_length'))
    except NoSuchElementException:
        select = Select(driver.find_element_by_name('resolutions-introduced_length'))

    select.select_by_visible_text('All')
    sleep(1)

    try:
        table = driver.find_element_by_id('resolutions-introduced').find_element_by_tag_name('tbody')
    except NoSuchElementException:
        table = driver.find_element_by_id('bills-introduced').find_element_by_tag_name('tbody')

    all_rows = table.find_elements_by_tag_name('tr')

    pbar = tqdm(all_rows[:40])
    for row in pbar:
        pbar.set_description('Grabbing URLs')
        link = row.find_element_by_tag_name('a').get_attribute('href')
        urls.append(link)

    driver.quit()
    return urls


def get_goverlytics_id(soup, row):
    """
    Generates goverlytics id based on state, session, and bill name.
    :param soup: soup object
    :param row: legislation row
    """

    bill_name = soup.find('div', {'class': 'bill-title'}).text.split()[0].replace('\t', '').replace('\n', '').replace('.', '')
    year = soup.find('dl', {'class': 'summary-table'}).find_all('dd')[-1].find('a').text.split()[1].split('/')[-1]
    row.goverlytics_id = f'{state_abbreviation}_{year}_{bill_name}'


def get_session(soup, row):
    """
    Gets legislation session.
    :param soup: soup object
    :param row: legislation row
    """

    year = soup.find('dl', {'class': 'summary-table'}).find_all('dd')[-1].find('a').text.split()[1].split('/')[-1]
    row.session = year


def get_bill_summary(soup, row):
    """
    Gets short bill summary.
    :param soup: soup object
    :param row: legislation row
    """

    bill_summary = soup.find('div', {'class': 'bill-title'}).find('h4', {'class': 'charge'}).text
    row.bill_summary = bill_summary


def get_bill_sponsor_and_committees(soup, row):
    """
    Gets sponsors and committees as they are sometimes in the same field.

    :param soup: soup object
    :param row: legislation row
    """

    sponsors_names = []
    committees = []

    sponsors = soup.find('dl', {'class': 'summary-table'}).find('dd').find_all('li')
    for sponsor in sponsors:
        try:
            sponsor = sponsor.find('a').text
            if sponsor == 'Less…':
                pass
            elif 'committee' in sponsor.lower():
                chamber = sponsor.split()[0]
                committees.append({'chamber': chamber, 'committee': sponsor})
            else:
                name = sponsor.split()
                sponsors_names.append(f'{name[-1]}, {name[1]}')
        except AttributeError:
            pass

    row.sponsors = sponsors_names
    row.committees = committees


def get_current_status(soup, row):
    """
    Get the current status of the bill.

    :param soup: soup object
    :param row: legislation row
    """

    current_status = soup.find('dl', {'class': 'summary-table'}).find_all('dd')[-1]
    row.current_status = current_status.text


def get_bill_text(soup, row):
    """
    Get the entire bill text.

    :param soup: soup object
    :param row: legislation row
    """

    bill_path = soup.find('ul', {'class': 'bill-path'})
    bill_texts = bill_path.find_all('li')
    latest_bill_text_endpoint = bill_texts[-1].find('a').get('href').replace(' ', '%20')
    link = BASE_URL + latest_bill_text_endpoint
    parsed = parser.from_file(link)

    row.bill_text = parsed['content']

@scraper_utils.Timer()
def get_bill_actions(url, row):
    """
    Get the bill actions and store as list of dictionaries.

    :param soup: soup object
    :param row: legislation row
    """
    actions = []
    driver = open_driver(url)
    select = Select(driver.find_element_by_name('bill-detailed-status-table_length'))
    select.select_by_visible_text('All')
    table = driver.find_element_by_id('bill-detailed-status-table').find_element_by_tag_name('tbody').find_elements_by_tag_name('tr')
    for tr in table:
        r = tr.find_elements_by_tag_name('td')
        sleep(3.5)
        actions.append({'action_by': str(r[0].text), 'date': str(r[1].text), 'description': str(r[-1].text)})
    row.actions = actions
    driver.quit()


@scraper_utils.Timer()
def get_bill_votes(url, row):
    votes = []
    driver = open_driver(url)
    sleep(2)
    tab = driver.find_element_by_class_name('nav nav-tabs bill-subtabs')
    # tab.find_elements_by_tag_name('li')[-1].click()
    # table = driver.find_element_by_id('bill-roll-call-table').find_element_by_tag_name('tbody').find_elements_by_tag_name('tr')
    # for tr in table:
    #     row = tr.find_elements_by_tag_name('td')
    #     vote_data_link = row[-1].find_element_by_tag_name('a').get_attribute('href')
    #     soup = make_soup(BASE_URL + vote_data_link)
    #     vote_table = soup.find('dl', {'class': 'summary-table export-details'}).find_all('dd')
    #     individual_votes = []
    #     vote_details = driver.find_element_by_id('roll-call-detail-table').find_element_by_tag_name('tbody').find_elements_by_tag_name('tr')
    #     for member in vote_details:
    #         member_details = member.find_elements_by_tag_name('td')
    #         member_endpoint = member_details[0].find_element_by_tag_name('a').get_attribute('href')
    #         soup_two = make_soup(BASE_URL + '/' + member_endpoint)
    #         name = " ".join(soup_two.find('h1').split()[1:])
    #         member_vote = member_details[-1].text
    #         individual_votes.append({'legislator': name, 'vote': member_vote})
    #
    #     votes.append({'date': vote_table[0].text, 'description': vote_table[1].text, 'yea': vote_table[2].text,
    #                   'nay': vote_table[3].text, 'absent': vote_table[4].text, 'passed': vote_table[5].text, 'nv': 0,
    #                   'chamber': row[0].text, 'votes': individual_votes})
    #
    # row.votes = votes


def scrape(url):
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)

    row = scraper_utils.initialize_row()
    row.source_url = url

    get_goverlytics_id(soup, row)
    # get_bill_text(soup, row)
    get_bill_sponsor_and_committees(soup, row)
    get_session(soup, row)
    # get_bill_actions(url, row)
    get_bill_votes(url, row)
    # get_bill_name(url, row)
    # get_date_introduced(url, row)
    # get_bill_type(url, row)
    # get_committees(url, row)
    get_current_status(soup, row)
    get_bill_summary(soup, row)

    pprint(row)
    return row


def main():
    bills_urls = get_urls(BILL_PATH)
    resolutions_urls = get_urls(RESOLUTIONS_PATH)
    all_urls = bills_urls + resolutions_urls

    with Pool() as pool:
        data = pool.map(scrape, all_urls)

    # data = [scrape(url) for url in urls]
    # scraper_utils.write_data(data, 'us_nm_legislation')


if __name__ == '__main__':
    main()
