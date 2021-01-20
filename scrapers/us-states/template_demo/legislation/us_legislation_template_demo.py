'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

from legislation_scraper_utils import LegislationScraperUtils, LegislationRow
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import urllib.parse as urlparse
from urllib.parse import parse_qs

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
legislator_table_name = str(configParser.get('scraperConfig', 'legislator_table_name'))

#Initialize database and scraper utils
db_user = str(configParser.get('databaseConfig', 'db_user'))
db_pass = str(configParser.get('databaseConfig', 'db_pass'))
db_host = str(configParser.get('databaseConfig', 'db_host'))
db_name = str(configParser.get('databaseConfig', 'db_name'))

Database.initialise(database=db_name, host=db_host, user=db_user, password=db_pass)

scraper_utils = LegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)

base_url = 'https://www.ilga.gov'

def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:
    
    path = '/legislation/grplist.asp?num1=64&num2=3933&DocTypeID=HB&GA=101&SessionId=109&SpecSess=1'
    scrape_url = base_url + path
    page = requests.get(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    table = soup.find('table', {'width': '490', 'border': '0', 'align': 'left'})

    for li in table.findAll('li'):
        urls.append(li.a['href'])
    
    return urls


def scrape(url):
    '''
    Insert logic here to scrape all URLs acquired in the get_urls() function.

    Do not worry about collecting the date_collected, state, and state_id values,
    as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.
    '''
    
    row = scraper_utils.initialize_row()

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:
    state_url = f'{base_url}{url}'
    row.state_url = state_url

    # Get useful query string parameters from URL
    parsed = urlparse.urlparse(url)
    url_qsp = parse_qs(parsed.query)

    doc_type = url_qsp['DocTypeID'][0]
    doc_num = url_qsp['DocNum'][0]
    session = url_qsp['SessionID'][0]

    bill_name = f'{doc_type}{doc_num.zfill(4)}'

    goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'
    url = f'us/{state_abbreviation}/legislation/{goverlytics_id}'

    row.goverlytics_id = goverlytics_id
    row.bill_name = bill_name
    row.session = session
    row.url = url

    chamber_origin = ''
    bill_type = ''
    if 'HB' == doc_type:
        chamber_origin = 'House'
        bill_type = 'Bill'
    # elif ...:
        # Check for other types like SB (senate bills), HRes (House Resolutions), etc.
        # For now we're only work with HB bills so we'll keep it simple

    row.chamber_origin = chamber_origin
    row.bill_type = bill_type

    # Begin scraping page
    page = requests.get(state_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    table = soup.find('table', {'width': '440', 'border': '0', 'align': 'left'})
    table_td = table.find('td', {'width': '100%'})

    # The Illinois state legislation website has their data stored in a weird way...
    # everything is stored in spans so we're gonna try pulling the data we need from
    # those. Your implementations will probably look quite a bit different than this.
    bill_description = ''
    bill_summary = ''
    spans = table_td.findAll('span')
    for idx, span in enumerate(spans):
        txt = span.text
        if 'Short Description:' in txt:
            bill_description = spans[idx + 1].text
        if 'Synopsis As Introduced' in txt:
            bill_summary = spans[idx + 1].text
    row.bill_description = bill_description
    row.bill_summary = bill_summary

    a_tag = table_td.findAll('a', href=True)
    sponsor_full_name = ''

    legislator_id = ''
    for a in a_tag:
        if '/house/' in a['href']:
            sponsor = a.text
            legislator_id = a['href'].split('=')[-2][:4]
            # print(legislator_id)

    # We'll now try to get the legislator goverlytics ID. Fortunately for us, this
    # site provides a unique identifier for each legislator, so I am able to do the
    # following:
    sponsor_id = scraper_utils.get_legislator_id(state_member_id=legislator_id)

    print(sponsor_id)


    return row

if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    data = [scrape(url) for url in urls[:10]]
    # with Pool() as pool:
    #     data = pool.map(scrape, urls)

    # # Once we collect the data, we'll write it to the database.
    # scraper_utils.insert_legislator_data_into_db(data)

    print('Complete!')
