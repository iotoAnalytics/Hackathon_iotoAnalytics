import os
import re
import sys

import multiprocessing
from bs4 import BeautifulSoup
from datetime import datetime
from multiprocessing import Pool
from nameparser import HumanName
from pathlib import Path
from pprint import pprint
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

import us_tn_legislation_utils as tn_utils
from scraper_utils import USStateLegislationScraperUtils

DEBUG_MODE = True

STATE_ABBREVIATION = 'TN'
DATABASE_TABLE_NAME = 'us_tn_legislation_test'
LEGISLATOR_TABLE_NAME = 'us_tn_legislators_test'

BASE_URL = 'https://wapp.capitol.tn.gov/apps/'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION, DATABASE_TABLE_NAME, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_urls():
    bill_index_urls = _get_bill_index_urls()
    bill_urls = _get_bill_urls_from_index(bill_index_urls)

    return bill_urls

def scrape(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    # TODO - goverlytics_id
    # TODO - source_id
    
    # bill_name
    _set_bill_name(row, soup)

    # TODO - session
    
    # date_introduced
    _set_date_introduced(row, soup)

    # source_url
    _set_source_url(row, url)
    
    # chamber_origin
    _set_chamber_origin(row, soup)

    # TODO - committees

    # bill_type
    _set_bill_type(row, soup)

    # TODO - bill_title

    # current_status
    _set_current_status(row, soup)

    # principal_sponsor_id & principal_sponsor
    _set_principal_sponsor(row, soup)

    # TODO - sponsors
    # TODO - sponsors_id
    # TODO - cosponsors
    # TODO - cosponsors_id
    # TODO - bill_text
    # TODO - bill_description
    # TODO - bill_summary
    # TODO - actions
    # TODO - votes
    # TODO - source_topic

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

def _set_bill_name(row, soup):
    bill_name = soup.select('#lblBillNumber')[0].text.strip()
    row.bill_name = bill_name

def _set_session():
    pass

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
    bill_name = soup.select('#lblBillNumber')[0].text.strip()
    bill_type_abrv = re.sub(' [0-9]+', '', bill_name)
    bill_type = tn_utils.BILL_TYPE_FULL.get(bill_type_abrv)
    row.bill_type = bill_type

def _set_current_status(row, soup):
    # Skip table headers
    current_status = (soup.select('#gvBillActionHistory')[0].find_all('tr')[1]
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

def main():
    print('\nSCRAPING TENNESSEE LEGISLATION\n')

    # Collect legislation urls
    print(DEBUG_MODE and 'Collecting legislation URLs...\n' or '', end='')
    # urls = get_urls()

    # Scrape data from collected URLs
    print(DEBUG_MODE and 'Scraping data from collected URLs...\n' or '', end='')
    # with Pool(NUM_POOL_PROCESSES) as pool:
    #     data = list(tqdm(pool.imap(scrape, urls)))
    data = [scrape('https://wapp.capitol.tn.gov/apps/BillInfo/default.aspx?BillNumber=HB0001&GA=112')]

    pprint(data, width=200)

if __name__ == '__main__':
    main()