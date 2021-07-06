# TODO - Scrape votes
# TODO - Scrape committees
# TODO - Scrape first extraordinary session (double check session naming)
# TODO - Refactor for error handling and code duplication
# TODO - Multiprocessing for fetching urls
# Unavailable data - source_id, bill_title, source_topic

import io
import os
import multiprocessing
import re
import sys
from datetime import datetime
from multiprocessing import Pool
from time import sleep

import pdfplumber
import requests
from bs4 import BeautifulSoup
from nameparser import HumanName
from pathlib import Path
from pprint import pprint
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

import us_tn_legislation_utils as tn_utils
from scraper_utils import USStateLegislationScraperUtils

DEBUG_MODE = False

STATE_ABBREVIATION = 'TN'
DATABASE_TABLE_NAME = 'us_tn_legislation_test'
LEGISLATOR_TABLE_NAME = 'us_tn_legislators'

BASE_URL = 'https://wapp.capitol.tn.gov/apps/'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)
WEBDRIVER_PATH = os.path.join('..', '..', '..', '..', '..', 'web_drivers', 'chrome_win_90.0.4430.24', 'chromedriver.exe')

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION, DATABASE_TABLE_NAME, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_urls():
    bill_index_urls = _get_bill_index_urls()
    bill_urls = _get_bill_urls_from_index(bill_index_urls)
    return bill_urls

def scrape(url):
    soup = _create_soup_from_selenium(url)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()
    
    try:
        _set_bill_name(row, soup)
        _set_session(row, url)
        _set_date_introduced(row, soup)
        _set_source_url(row, url)
        _set_chamber_origin(row, soup)
        # TODO - committees
        _set_bill_type(row, soup)
        _set_current_status(row, soup)
        _set_principal_sponsor(row, soup)
        _set_sponsors(row, soup)
        # TODO - cosponsors
        # TODO - cosponsors_id
        _set_bill_text(row, soup)
        _set_description(row, soup)
        _set_bill_summary(row, soup)
        _set_actions(row, soup)
        # TODO - votes
        # _set_votes(row, soup)
        _set_goverlytics_id(row)
    except Exception:
        print(f'Problem occurred with: {url}')

    return row
    
def _get_bill_index_urls():
    bill_indexes_path = 'indexes/'

    soup = _create_soup(BASE_URL + bill_indexes_path, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    bill_indexes = soup.find('table').find('tbody').find_all('a')
    bill_index_urls = [BASE_URL + bill_indexes_path + bill_index.get('href')
        for bill_index in bill_indexes]

    return bill_index_urls

def _get_bill_urls_from_index(bill_index_urls):
    bill_urls = []

    for bill_index_url in tqdm(bill_index_urls):
        soup = _create_soup(bill_index_url, SOUP_PARSER_TYPE)
        scraper_utils.crawl_delay(crawl_delay)

        bills = soup.find('table').find_all('a')
        bill_urls += [BASE_URL + bill.get('href').replace('../', '')
            for bill in bills]

    return bill_urls

def _create_soup(url, soup_parser_type):
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _create_soup_from_selenium(url):
    options = Options()
    options.add_argument("--headless")

    if not DEBUG_MODE:
        options.add_argument("--log-level=3")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

    driver = webdriver.Chrome(WEBDRIVER_PATH, options=options)
    driver.switch_to.default_content()
    driver.get(url)
    driver.maximize_window()
    sleep(2)

    # Open all texts to be parsed properly
    _click_element_by_id(driver, 'lnkShowCoPrimes')
    _click_element_by_id(driver, 'lnkShowCaptionText')
    
    html = driver.page_source
    soup = BeautifulSoup(html, SOUP_PARSER_TYPE)
    driver.quit()

    return soup

def _click_element_by_id(driver, element_id):
    try:
        btn = driver.find_element_by_id(element_id)
        btn.click()
    except NoSuchElementException:
        # Element does not exist
        pass
    else:
        sleep(2)
        return btn

def _set_bill_name(row, soup):
    bill_name = soup.select('#udpBillInfo > h2 > a')[-1].text.strip()
    bill_name = bill_name.replace(' ', '')
    row.bill_name = bill_name

def _set_session(row, url):
    session = re.search(r'GA=([0-9]+)', url).group(1)
    row.session = session

def _set_date_introduced(row, soup):
    date_introduced_str = (soup.select('#gvBillActionHistory')[0].find_all('tr')[-1]
        .find_all('td')[-1].text
    )
    date_introduced = datetime.strptime(date_introduced_str, '%m/%d/%Y')
    row.date_introduced = date_introduced

def _set_source_url(row, url):
    row.source_url = url

def _set_chamber_origin(row, soup):
    legislator_url = soup.select('#lblBillPrimeSponsor > a')[0].get('href')
    search = re.search('.gov/(house|senate)/members', legislator_url)
    chamber_origin = search.group(1).title()
    row.chamber_origin = chamber_origin

def _set_bill_type(row, soup):
    bill_name = soup.select('#udpBillInfo > h2 > a')[-1].text.strip()
    bill_type_abrv = re.sub(' [0-9]+', '', bill_name)
    bill_type = tn_utils.BILL_TYPE_FULL.get(bill_type_abrv)
    row.bill_type = bill_type

def _set_current_status(row, soup):
    current_status_idx = 1
    current_status = (soup.select('#gvBillActionHistory')[0].find_all('tr')[current_status_idx]
        .find_all('td')[0].text.strip()
    )
    row.current_status = current_status

def _set_principal_sponsor(row, soup):
    sponsor_element = soup.select('#lblBillPrimeSponsor')[0].find('a')

    sponsor_str = sponsor_element.text.strip()
    sponsor = sponsor_str.replace('*', '')

    sponsor_url = sponsor_element.get('href').replace('http://', 'https://').lower()
    gov_id = scraper_utils.get_legislator_id(source_url=sponsor_url)
    
    row.principal_sponsor = sponsor
    row.principal_sponsor_id = gov_id

def _set_sponsors(row, soup):
    try:
        sponsors_element = soup.select('#lblBillCoPrimeSponsor')[0]
    except IndexError:
        # No sponsors exists
        return
    
    sponsors_str = sponsors_element.text.strip()
    sponsors = [sponsor for sponsor in sponsors_str.split(', ') if sponsor]
    sponsors_id = []

    chamber = _get_sponsor_chamber(soup)

    for sponsor in sponsors:
        gov_id = _get_sponsor_id(sponsor, chamber)
        sponsors_id.append(gov_id)

    row.sponsors = sponsors
    row.sponsors_id = sponsors_id

def _get_sponsor_chamber(soup):
    prime_sponsor_element = soup.select('#lblBillPrimeSponsor')[0].find('a')
    prime_sponsor_url = prime_sponsor_element.get('href')
    chamber = re.search(r'.gov/(house|senate)/members', prime_sponsor_url).group(1)
    return chamber

def _get_sponsor_id(name, chamber):
    role = tn_utils.CHAMBER_TO_ROLE.get(chamber)

    # Search assuming name is "name_last"
    gov_id = scraper_utils.get_legislator_id(name_last=name, role=role)
    
    # Otherwise, search assuming name is "name_last, name_first"
    if not gov_id:
        name_last, name_first = sponsor.split()
        gov_id = scraper_utils.legislators_search_startswith('goverlytics_id','name_first',
            name_first, name_last=name_last, role=role)

    return gov_id

def _set_bill_text(row, soup):
    bill_element = soup.select('#udpBillInfo > h2 > a')[-1]
    bill_text_url = bill_element.get('href')
    bill_text_url = bill_text_url.replace('http', 'https')

    response = requests.get(bill_text_url, stream=True)
    pdf = pdfplumber.open(io.BytesIO(response.content))
    
    bill_text = _combine_pages_into_text(pdf)
    row.bill_text = bill_text

def _combine_pages_into_text(pdf):
    bill_text = ''

    for page in pdf.pages:
        if page_text:= page.extract_text():
            bill_text += page_text
    
    return bill_text

def _set_description(row, soup):
    try:
        bill_description = soup.select('#lblCaptionText')[0].text.strip()
        row.bill_description = bill_description
    except IndexError:
        # Description does not exist
        raise

def _set_bill_summary(row, soup):
    bill_summary = soup.select('#lblSummary')[0].text.strip()
    row.bill_summary = bill_summary

def _set_actions(row, soup):
    actions = []
    actions_table = soup.select('#gvBillActionHistory')[0]
    action_rows = actions_table.find_all('tr')

    # Skip table heading
    for action_row in action_rows[1:]:
        table_class_attr = action_row.get('class')
        action_by = table_class_attr[0] if table_class_attr else ''
        description, date = action_row.find_all('td')
        action = {
            'date': datetime.strptime(date.text, r'%m/%d/%Y'),
            'action_by': action_by,
            'description': description.text
        }
        actions.append(action)

    row.actions = actions

def _set_votes(row, soup):
    # votes = soup.select('#lblHouseVoteData')[0].text.strip()
    pass

def _set_goverlytics_id(row):
    goverlytics_id = f'{STATE_ABBREVIATION}_{row.session}_{row.bill_name}'
    row.goverlytics_id = goverlytics_id

def main():
    print('\nSCRAPING TENNESSEE LEGISLATION\n')

    # Collect legislation urls
    print(DEBUG_MODE and 'Collecting legislation URLs...\n' or '', end='')
    urls = get_urls()

    # Scrape data from collected URLs
    print(DEBUG_MODE and 'Scraping data from collected URLs...\n' or '', end='')
    with Pool(NUM_POOL_PROCESSES) as pool:
        data = list(tqdm(pool.imap(scrape, urls)))
    # data = [scrape('https://wapp.capitol.tn.gov/apps/BillInfo/default.aspx?BillNumber=HB0001&GA=112')]
    # data = [scrape('https://wapp.capitol.tn.gov/apps/BillInfo/default.aspx?BillNumber=HB0099&GA=112')]
    # data = [scrape('https://wapp.capitol.tn.gov/apps/BillInfo/default.aspx?BillNumber=SB0530&GA=112')]

    print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
    if not DEBUG_MODE:
        scraper_utils.write_data(data)

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()