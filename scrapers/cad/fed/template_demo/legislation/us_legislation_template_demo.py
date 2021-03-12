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

from legislation_scraper_utils import USStateLegislationScraperUtils, USStateLegislationRow
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
from pprint import pprint
import datetime
import boto3

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
legislator_table_name = str(configParser.get('scraperConfig', 'legislator_table_name'))

scraper_utils = USStateLegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)

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
    soup = BeautifulSoup(page.content, 'lxml')

    # The Illinois state legislation website has their data stored in a weird way...
    # everything is stored in spans so we're gonna try pulling the data we need from
    # those. Your implementations will probably look quite a bit different than this.

    # Get bill description and summary
    bill_description = ''
    bill_summary = ''
    spans = soup.findAll('span')
    for idx, span in enumerate(spans):
        txt = span.text
        if 'Short Description:' in txt:
            bill_description = spans[idx + 1].text
        if 'Synopsis As Introduced' in txt:
            bill_summary = spans[idx + 1].text.strip()
    row.bill_description = bill_description
    row.bill_summary = bill_summary

    # Get bill sponsors
    table = soup.find('table', {'width': '440', 'border': '0', 'align': 'left'})
    table_td = table.find('td', {'width': '100%'})

    a_tag = table_td.findAll('a', href=True)
    sponsors = []
    for a in a_tag:
        if '/house/Rep.asp' in a['href'] or '/senate/Senator.asp' in a['href']:
            sponsors.append(a.text)

    # # We'll now try to get the legislator goverlytics ID. Fortunately for us, this
    # # site provides a unique identifier for each legislator. Normally we would do
    # # the following:
    # sponsor_id = scraper_utils.get_legislator_id(state_member_id=legislator_id)
    # # However, since this is often not the case, we will search for the id using the
    # # legislator name. We are given the legislator's full name, but if you are given
    # # only the legislator initials and last name, which is more often the case, be sure to
    # # use the legislators_search_startswith() method, which might look something like this:
    # sponsor_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', first_initial, name_last=name_last)

    sponsors_id = []
    for sponsor in sponsors:
        hn = HumanName(sponsor)
        name_first = hn.first
        name_middle = hn.middle
        name_last = hn.last
        name_suffix = hn.suffix

        search_for = dict(name_first=name_first, name_middle=name_middle, name_last=name_last, name_suffix=name_suffix)

        sponsor_id = scraper_utils.get_legislator_id(**search_for)

        # Some sponsor IDs weren't found, so we won't include these.
        # If you are unable to find legislators based on the provided search criteria, be
        # sure to investigate. Check the database and make sure things like names match
        # exactly, including case and diacritics.
        if sponsor_id is not None:
            sponsors_id.append(sponsor_id)

    row.sponsors = sponsors
    row.sponsors_id = sponsors_id

    # Get actions
    actions_table = soup.findAll('table', {'width': '600', 'cellspacing': '0', 'cellpadding': '2', 'bordercolor': 'black', 'border':'1'})[1]

    action_date = ''
    action_by = ''
    action_description = ''
    actions = []
    number_of_columns = 3
    # Skip the header row
    for idx, td in enumerate(actions_table.findAll('td')[3:]):
        # With this type of method, normally you would search by 'tr' and then grab the value
        # from each 'td' in the row, but for some reason, beautiful soup wasn't able to find
        # the 'tr' so I had to get the value using a different, less intuitive method.
        mod = idx % number_of_columns
        if mod == 0:
            action_date = td.text.strip()
        if mod == 1:
            action_by = td.text.strip()
        if mod == 2:
            action_description = td.text.strip()
            actions.append(dict(date=action_date, action_by=action_by, description=action_description))
    
    # We can get the date introduced from the first action, and the current status from
    # the most recent action.
    date_introduced = None
    current_status = ''
    if len(actions) > 0:
        date_introduced=datetime.datetime.strptime(actions[0]['date'], '%m/%d/%Y')
        current_status = actions[-1]['description']

    row.actions = actions
    row.current_status = current_status
    row.date_introduced = date_introduced

    # There's more data on other pages we can colelct, but we have enough data for this demo!

    return row

if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')

    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape(url) for url in urls
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database.
    scraper_utils.insert_legislation_data_into_db(data)

    print('Complete!')
