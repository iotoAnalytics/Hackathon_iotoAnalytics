# TODO - Consider trying to get all votes data for senate committee oddities
# BUG - HB9999 should not be scraped

# Unavailable data - source_id, committees, source_topic

from pathlib import Path
import os
import sys

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

# import boto3
from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
# import requests
from multiprocessing import Pool
from pprint import pprint
# from database import Database
from nameparser import HumanName
import re
from datetime import datetime
from tqdm import tqdm

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from time import sleep

import traceback

import pdfplumber
import requests
import io

import us_ok_legislation_utils as OKLegislationUtils
from us_ok_legislation_votes_parser import OKLegislationVotesParser

# These urls shouldn't be in the OK database
ODDITIES = [
    'http://www.oklegislature.gov/BillInfo.aspx?Bill=SB9001&session=2100',
    'http://www.oklegislature.gov/BillInfo.aspx?Bill=SB9999&session=2100',
    'http://www.oklegislature.gov/BillInfo.aspx?Bill=SB10852&session=2100',
    'http://www.oklegislature.gov/BillInfo.aspx?Bill=HB9999&session=2100',
]

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

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION, DATABASE_TABLE_NAME, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

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

    _set_goverlytics_id(row, url)
    _set_bill_name(row, soup)
    
    try:
        _set_session(row, url)
        _set_date_introduced(row, soup)
        _set_source_url(row, url)
        _set_chamber_origin(row, soup)
        _set_bill_type(row, soup)
        _set_principal_sponsor(row, soup)
        _set_sponsors(row, soup)
        _set_cosponsors(row, soup)

        _set_bill_text(row, soup)
        _set_bill_description(row, soup)
        _set_bill_summary(row, soup)
        _set_actions(row, soup)
        _set_votes(row, soup)
    except:
        print(f'Error occurred with {row.bill_name}')
        pass
    finally:
        return row

# Merge data obtained from scrape_regular_session (current status, title)
def merge_all_scrape_regular_session_data(legislation_data, scrape_regular_session_data):
    for data in scrape_regular_session_data:
        source_url = data['url']
        legislation_row = _get_legislation_row(legislation_data, source_url)
        if legislation_row != None:
            _merge_scrape_regular_session_data(legislation_row, data)

def _create_soup(url, soup_parser_type):
    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_goverlytics_id(row, url):
    pattern = re.compile('Bill=([A-Za-z0-9]+)\&session=([A-Za-z0-9]+)')
    bill_name = pattern.search(url).group(1)
    session_code = pattern.search(url).group(2)

    goverlytics_id = STATE_ABBREVIATION + '_' + session_code + '_' + bill_name
    row.goverlytics_id = goverlytics_id
    
def _set_bill_name(row, soup):
    bill_name = soup.find('a', {'id': 'ctl00_ContentPlaceHolder1_lnkIntroduced'}).text
    bill_name = bill_name.replace(' ', '')
    row.bill_name = bill_name

def _set_session(row, url):
    pattern = re.compile('session=[A-Za-z0-9]+')
    session_code = pattern.search(url).group(0)
    session_code = session_code.replace('session=', '')
    session = OKLegislationUtils.get_session(session_code)
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
    chamber_origin = OKLegislationUtils.get_chamber(chamber_origin_code)
    row.chamber_origin = chamber_origin

def _set_bill_type(row, soup):
    bill_name = soup.find('a', {'id': 'ctl00_ContentPlaceHolder1_lnkIntroduced'}).text
    bill_code = bill_name.split(' ')[0]
    bill_type = OKLegislationUtils.get_bill_type(bill_code)
    row.bill_type = bill_type

def _set_bill_title(row, title):
    row.bill_title = title

def _set_current_status(row, status_code):
    row.current_status = status_code

def _set_principal_sponsor(row, soup):
    # OK has only one principal sponsor for a legislation
    sponsor_soup = soup.find('a', {'id': 'ctl00_ContentPlaceHolder1_lnkAuth'})
    
    sponsor_str = sponsor_soup.text
    sponsor_url = sponsor_soup.get('href')

    sponsor_data = _get_sponsor_data(sponsor_str, sponsor_url)

    row.principal_sponsor = sponsor_data['name']
    row.principal_sponsor_id = sponsor_data['id']

def _set_sponsors(row, soup):
    # OK can have at most one sponsor for a legislation
    sponsor_soup = soup.find('a', {'id': 'ctl00_ContentPlaceHolder1_lnkOtherAuth'})
    
    if sponsor_soup == None:
        return
    
    sponsor_str = sponsor_soup.text
    sponsor_url = sponsor_soup.get('href')

    sponsor_data = _get_sponsor_data(sponsor_str, sponsor_url)

    row.sponsors = [sponsor_data['name']]
    row.sponsors_id = [sponsor_data['id']]

def _set_cosponsors(row, soup):
    # Has the minimum two lines:
    # Authors/Co Authors for
    # ***To be added when the next official action is taken on the measure.***

    table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_TabContainer1_TabPanel6_tblCoAuth'})
    # table_rows = table.find_all('tr')[1:-2]
    table_rows = table.find_all('tr', {'align', 'left'})[1:-2]

    # Return if table is not populated yet
    if len(table_rows) < 3:
        return

    cosponsors = []
    cosponsors_id = []

    for table_row in table_rows:
        sponsor_data = _get_sponsor_data(table_row.text)

        if sponsor_data == None:
            continue
        
        cosponsors.append(sponsor_data['name'])
        cosponsors_id.append(sponsor_data['id'])

    row.cosponsors = cosponsors
    row.cosponsors_id = cosponsors_id

def _set_bill_text(row, soup):
    table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_TabContainer1_TabPanel4_tblVersions'})
    table_rows = table.find_all('a')

    # Return if no data exists
    if len(table_rows) < 1:
        return

    # Get url (PDF) of bill text
    url = None
    for table_row in table_rows:
        if 'final version' in table_row.text:
            url = table_row.get('href')
    else:
        if url == None:
            version_soup = table_rows[0]
            url = table_row.get('href')

    # Read from PDF
    response = requests.get(url, stream = True)
    pdf = pdfplumber.open(io.BytesIO(response.content))

    bill_text = ''
    for page in pdf.pages:
        page_text = page.extract_text()
        if page_text != None:
            bill_text += page_text

    row.bill_text = bill_text

def _set_bill_description(row, soup):
    bill_description = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_txtST'}).text
    row.bill_description = bill_description

def _set_bill_summary(row, soup):
    table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_TabContainer1_TabPanel3_tblBillSum'})
    table_rows = table.find_all('a')

    # Return if no data exists
    if len(table_rows) < 1:
        return

    # Get url (PDF) of bill text
    url = None
    for table_row in table_rows:
        if 'Engrossed' in table_row.text:
            url = table_row.get('href')
    else:
        if url == None:
            version_soup = table_rows[0]
            url = table_row.get('href')

    # Read from PDF
    response = requests.get(url, stream = True)
    pdf = pdfplumber.open(io.BytesIO(response.content))

    bill_summary = ''
    for page in pdf.pages:
        page_text = page.extract_text()
        if page_text != None:
            bill_summary += page_text

    row.bill_summary = bill_summary

def _set_actions(row, soup):
    table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_TabContainer1_TabPanel1_tblHouseActions'})
    table_rows = table.find_all('tr')[2:]

    # Remove rows with odd whitespace only
    table_rows = list(filter(lambda x: x.text != '\n\n', table_rows))

    # Return if no actions data exists
    if len(table_rows) < 1:
        return

    actions = []

    for table_row in table_rows:
        fields = table_row.find_all('td')

        action = {
            'date': datetime.strptime(fields[2].text.strip(), '%m/%d/%Y'),
            'action_by': OKLegislationUtils.get_chamber(fields[3].text),
            'description': fields[0].text,
        }
        actions.append(action)

    row.actions = actions

def _set_votes(row, soup):
    table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_TabContainer1_TabPanel5_tblVotes'})
    table_rows = table.find_all('a')

    # Return if no vote data exists
    if len(table_rows) < 1:
        return

    votes = []

    for table_row in table_rows:
        try:
            url = table_row.get('href')
            vote_soup = _create_soup(url, SOUP_PARSER_TYPE)
            chamber_votes = OKLegislationVotesParser().get_votes_data(vote_soup)
            votes = votes + chamber_votes
        except:
            print(f'Could not get all votes data for {row.bill_name}')
    
    row.votes = votes

def _get_sponsor_data(sponsor_str, sponsor_url=None):
    """
    Returns name and id of sponsor

    Notes:
        sponsor_str must be one of the following form:
            Dossett (J.J.)
            Hardin (David) (H)
            Luttrell (H)

    Parameters:
        sponsor_str (str): Sponsor text
        sponsor_url (str, optional): URL to sponsor's page
        
    Returns:
        {
            'id' (str),
            'name' (str)
        }
    """

    sponsor = _format_sponsor_data(sponsor_str, sponsor_url)

    if sponsor == None:
        return

    sponsor_last_name = sponsor['last']
    sponsor_first_name = sponsor['first']
    sponsor_name = sponsor['name']
    sponsor_role = sponsor['role']

    search_query = {
        'name_last': sponsor_last_name,
        'state': STATE_ABBREVIATION,
        'role': sponsor_role,
    }

    if sponsor_first_name != None:
        search_query['name_first'] = sponsor_first_name

    gov_id = scraper_utils.get_legislator_id(**search_query)

    sponsor_data = {
        'name': sponsor_name,
        'id': gov_id,
    }

    return sponsor_data

def _format_sponsor_data(sponsor_str, sponsor_url=None):        
    sponsor_str = re.sub('[/(/)\n]', '', sponsor_str)
    sponsor_data = sponsor_str.split()

    if sponsor_data == None or len(sponsor_data) == 0:
        return

    is_common_name = len(sponsor_data) > 2

    sponsor = {
        'last': sponsor_data[0],
        'first': sponsor_data[1] if is_common_name else None,
        'name': f'{sponsor_data[1]} {sponsor_data[0]}' if is_common_name else sponsor_data[0],
    }

    if sponsor_url == None:
        sponsor['role'] = OKLegislationUtils.get_sponsor_role_from_abbr(sponsor_data[2]) if is_common_name else OKLegislationUtils.get_sponsor_role_from_abbr(sponsor_data[1])
    else:
        sponsor['role'] = OKLegislationUtils.get_sponsor_role_from_url(sponsor_url)

    return sponsor

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
    # Collect current status, title, and urls of regular session 2021
    print(DEBUG_MODE and 'Collecting scraping data for regular session 2021...\n' or '', end='')
    regular_session_data = scrape_regular_session()

    # Remove oddities
    for reg_ses in regular_session_data:
        if reg_ses['url'] in ODDITIES:
            regular_session_data.remove(reg_ses)

    # Retrieve urls into one list
    print(DEBUG_MODE and 'Collecting all urls for OK legislation...\n' or '', end='')
    urls = [data['url']
        for data in regular_session_data] 

    # Scrape each url
    print(DEBUG_MODE and 'Begin scraping each urls...\n' or '', end='')
    with Pool(NUM_POOL_THREADS) as pool:
        data = list(tqdm(pool.imap(scrape, urls[0:1000])))

    # pprint(urls[2000:], width=200)

    # Merge current status and title
    print(DEBUG_MODE and 'Merging current status and title...\n' or '', end='')
    merge_all_scrape_regular_session_data(data, regular_session_data)

    # Write to database
    print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
    scraper_utils.write_data(data[222:224])
    print(data[222:224])


if __name__ == '__main__':
    main()