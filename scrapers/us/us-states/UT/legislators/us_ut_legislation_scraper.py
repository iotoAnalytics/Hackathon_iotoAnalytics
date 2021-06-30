import sys
import os
from pprint import pprint
from pathlib import Path
from nameparser import HumanName
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from multiprocessing import Pool
from time import sleep
from tika import parser
from scraper_utils import USStateLegislationScraperUtils

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))


BASE_URL_2021 = 'https://le.utah.gov/asp/billsintro/SubResults.asp?Listbox4=ALL'

state_abbreviation = 'VT'
database_table_name = 'us_ut_legislation'
legislator_table_name = 'us_ut_legislators'

scraper_utils = USStateLegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL_2021)


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


@scraper_utils.Timer()
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
def get_urls(url):
    urls = []
    soup = make_soup(url)
    form = soup.find('form', {'name': 'thisForm'})
    link_tables = form.find_all('div', {'class': 'subresults'})

    for div in link_tables:
        links = div.find('table').find_all('tr')[1:]
        for link in links:
            url = link.find('a').get('href')
            if url not in urls:
                urls.append(url)
    return urls


def get_goverlytics_id(soup, row):
    bread = soup.find('ul', {'id': 'breadcrumb'})
    year = bread.find_all('li')[1].text.split()[0]
    bill_name = bread.find_all('li')[-1].text.replace('.', '').replace(' ', '')
    gov_id = f'ut_{year}_{bill_name}'
    row.goverlytics_id = gov_id


def get_bill_name(soup, row):
    bread = soup.find('ul', {'id': 'breadcrumb'})
    bill_name = bread.find_all('li')[-1].text.replace('.', '').replace(' ', '')
    row.bill_name = bill_name


def scrape(url):
    row = scraper_utils.initialize_row()
    soup = make_soup(url)
    get_goverlytics_id(soup, row)
    # todo get_bill_text(soup, row)
    # todo get_bill_sponsor(soup, row)
    # todo get_bill_session(soup, row)
    # todo get_bill_actions(url, row)
    # todo get_bill_votes(url, row)
    # todo get_bill_name(soup, row)
    # todo get_bill_type(url, row)
    # todo get_bill_committees(url, soup, row)
    # todo get_current_status(soup, row)
    # todo get_bill_summary(soup, row)
    # todo get_date_introduced(url, row)
    return row


def main():
    urls = get_urls(BASE_URL_2021)

    with Pool() as pool:
        data = pool.map(scrape, urls)


if __name__ == '__main__':
    main()
