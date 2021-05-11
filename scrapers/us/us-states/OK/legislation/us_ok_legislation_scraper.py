from pathlib import Path
import os
import sys

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

import boto3
from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import re
from datetime import datetime
from nameparser import HumanName
from pprint import pprint

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from time import sleep

from us_ok_legislation_utils import get_session, get_chamber, get_bill_type, get_status

BASE_URL = 'http://webserver1.lsb.state.ok.us/WebApplication3/WebForm1.aspx'
SOUP_PARSER_TYPE = 'lxml'

STATE_ABBREVIATION = 'OK'
DATABASE_TABLE_NAME = 'us_ok_legislation_test'
LEGISLATOR_TABLE_NAME = 'us_ok_legislators_test'

DEBUG_MODE = True
NUM_POOL_THREADS = 10
CURRENT_YEAR = datetime.now().year

PATH = '../../../../../web_drivers/chrome_win_90.0.4430.24/chromedriver.exe'

header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
options = Options()
options.headless = True

scraper_utils = USStateLegislationScraperUtils('OK', 'us_ok_legislation_test', 'us_ok_legislators_test')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

# Senate Bill, Senate Resolution, Senate Concurrent Resolution, House Bill, House Concurrent Resolution, House Resolution
# 1R - Regular Session, 2R - 2nd Regular Session, 1S - Special Session

def scrape_regular_session():
    driver = webdriver.Chrome(PATH, options=options)
    driver.switch_to.default_content()
    driver.get(BASE_URL)
    driver.maximize_window()
    sleep(2)

    # Regular sessions only
    all_types_btn = driver.find_element_by_id('cbxAllTypes')
    all_types_btn.click()
    submit_btn = driver.find_element_by_id('Button1')
    submit_btn.click()
    sleep(3)
    
    html = driver.page_source
    regular_session_soup = BeautifulSoup(html, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table = regular_session_soup.find('table').find('tbody')
    table_rows = table.find_all('tr')[2:]
    # pprint(table_rows[0])

    data = []

    for row in table_rows:
        fields = row.find_all('td')
        legislation = {
            'measure': fields[0].text,
            'url': fields[0].find('a').get('href'),
            'flags': fields[1].text,
            'chamber': fields[2].text,
            'status': fields[3].text.strip(),
            'date': fields[4].text,
            'title': fields[5].text.replace('\n', ''),
        }
        data.append(legislation)

    driver.quit()

    return data

def get_urls(data):
    urls = []

    return

def scrape(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    # TODO - source_id

    # bill_name
    _set_bill_name(row, soup)

    # session
    _set_session(row, url)

    # date_introduced
    _set_date_introduced(row, soup)

    # source_url (U)
    _set_source_url(row, url)

    # chamber_origin
    _set_chamber_origin(row, soup)

    # TODO - committees

    #  bill_type
    _set_bill_type(row, soup)

    # TODO - bill_title (Performed by merge method)

    # TODO - current_status (Performed by merge method)

    # TODO - principal_sponsor_id
    # TODO - principal_sponsor
    # TODO - sponsors
    # TODO - sponsors_id
    # TODO - cosponsors
    # TODO - cosponsors_id
    # TODO - bill_text

    # TODO - bill_description
    _set_bill_description(row, soup)

    # TODO - bill_summary

    # actions (lots of data, uncomment for production)
    # _set_actions(row, soup)

    # TODO - votes
    # TODO - source_topic
    
    pprint(row)

    return row

# Merge data obtained from scrape_regular_session (current status, title)
def merge_all_scrape_regular_session_data(legislation_data, scrape_regular_session_data):
    for data in scrape_regular_session_data:
        source_url = data['url']
        legislation_row = _get_legislation_row(legislation_data, source_url)
        if legislation_row != None:
            # print(legislation_row)
            _merge_scrape_regular_session_data(legislation_row, data)

def _create_soup(url, soup_parser_type):
    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_bill_name(row, soup):
    bill_name = soup.find('a', {'id': 'ctl00_ContentPlaceHolder1_lnkIntroduced'}).text
    row.bill_name = bill_name

def _set_session(row, url):
    pattern = re.compile('session=[A-Za-z0-9]+')
    session_code = pattern.search(url).group(0)
    session_code = int(session_code.replace('session=', ''))
    session = get_session(session_code)
    row.session = session

def _set_date_introduced(row, soup):
    table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_TabContainer1_TabPanel1_tblHouseActions'})
    date_introduced_row = table.find_all('tr')[2]
    date_introduced_str = date_introduced_row.find_all('td')[2].text
    date_introduced = datetime.strptime(date_introduced_str, '%m/%d/%Y')
    row.date_introduced = date_introduced

def _set_source_url(row, url):
    row.source_url = url

def _set_chamber_origin(row, soup):
    table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_TabContainer1_TabPanel1_tblHouseActions'})
    chamber_origin_row = table.find_all('tr')[2]
    chamber_origin_code = chamber_origin_row.find_all('td')[3].text
    chamber_origin = get_chamber(chamber_origin_code)
    row.chamber_origin = chamber_origin

# Committees

def _set_bill_type(row, soup):
    bill_name = soup.find('a', {'id': 'ctl00_ContentPlaceHolder1_lnkIntroduced'}).text
    bill_code = bill_name.split(' ')[0]
    bill_type = get_bill_type(bill_code)
    row.bill_type = bill_type

def _set_bill_title(row, title):
    row.bill_title = title

def _set_current_status(row, status_code):
    current_status = get_status(status_code)
    row.current_status = current_status

def _set_bill_description(row, soup):
    bill_description = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_txtST'}).text
    row.bill_description = bill_description

def _set_actions(row, soup):
    table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_TabContainer1_TabPanel1_tblHouseActions'})
    table_rows = table.find_all('tr')[2:]

    actions = []

    for table_row in table_rows:
        fields = table_row.find_all('td')

        # Action, Journal Page, Date, Chamber
        num_fields = len(fields)
        if num_fields == 4:
            # print(datetime.strptime(columns[2].text, '%m/%d/%Y'))
            action = {
                'date': datetime.strptime(fields[2].text.strip(), '%m/%d/%Y'),
                'action_by': get_chamber(fields[3].text),
                'description': fields[0].text,
            }
            actions.append(action)

    row.actions = actions

def _merge_scrape_regular_session_data(legislation_row, scrape_regular_session_data):
    title = scrape_regular_session_data['title']
    status = scrape_regular_session_data['status']

    _set_bill_title(legislation_row, title)
    _set_current_status(legislation_row, status)

def _get_legislation_row(legislation_data, source_url):
    for row in legislation_data:
        if row.source_url == source_url:
            return row

def main():

    # regular_session_data = scrape_regular_session()

    # urls = [data['url']
    #     for data in regular_session_data] 
    # pprint(data[0])

    test_url = 'http://www.oklegislature.gov/BillInfo.aspx?Bill=SR6&session=2100'
    data = [scrape(test_url)]

    # Merge current status and title
    # merge_all_scrape_regular_session_data(data, regular_session_data[0:10])

if __name__ == '__main__':
    main()