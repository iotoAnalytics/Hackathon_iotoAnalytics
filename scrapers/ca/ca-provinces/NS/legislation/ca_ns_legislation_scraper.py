'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

import boto3
import datetime
from pprint import pprint
from urllib.parse import parse_qs
import urllib.parse as urlparse
import re
from nameparser import HumanName
import configparser
from database import Database
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
from scraper_utils import CAProvinceTerrLegislationScraperUtils


prov_terr_abbreviation = 'NS'
database_table_name = 'ca_ns_legislation'
legislator_table_name = 'ca_ns_legislators'
scraper_utils = CAProvinceTerrLegislationScraperUtils(prov_terr_abbreviation,
                                                      database_table_name,
                                                      legislator_table_name)

base_url = 'https://nslegislature.ca'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:
    path = '/legislative-business/bills-statutes/bills/'
    scrape_url = base_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    table = soup.findAll('td', {'class': 'views-field-field-short-title-1'})

    for td in table:
        urls.append(td.a['href'])
    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)
    return urls


def get_bill_name(url, row):
    bill_number = url.split('bill-')[1]
    zero_filled_number = bill_number.zfill(3)
    bill_name = 'b' + zero_filled_number
    row.bill_name = bill_name
    return bill_name


def get_session(main_div, row):
    table = main_div.find('table', {'class': 'views-table'})
    session = table.findAll('td')[2].text
    row.session = session
    return_session = session.split(', ')[1]
    return_session = return_session.replace(' ', '')
    return return_session


def get_bill_type(main_div, row):
    table = main_div.find('table', {'class': 'views-table'})
    bill_type = table.findAll('td')[1].text
    row.bill_type = bill_type


def get_bill_title(main_div, row):
    title = main_div.find('h1')
    title = title.text.split(' - ')[0]
    try:
        title = title.split('- Bill')[0]
    except Exception:
        pass
    title = title.replace('\n', '')
    row.bill_title = title


def get_current_status(main_div, row):
    table = main_div.find('table', {'class': 'bill-metadata-table'})
    table_row = table.findAll('tr')
    status = table_row[-1].findAll('td')[0].text
    if "Law Amendments" in status:
        status = table_row[-2].findAll('td')[0].text
    row.current_status = status


def get_actions(main_div, row):
    actions = []
    table = main_div.find('table', {'class': 'bill-metadata-table'})
    table_row = table.findAll('tr')
    for tr in reversed(table_row):
        status = tr.findAll('td')[0].text
        date = tr.findAll('td')[1].text
        if status:
            if date:
                action = {'date': date, 'action_by': 'Legislative Assembly', 'description': status}
                actions.append(action)
    print(actions)

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
    source_url = base_url + url
    row.source_url = source_url
    row.region = scraper_utils.get_region(prov_terr_abbreviation)

    page = scraper_utils.request(source_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    main_div = soup.find('div', {'class': 'panel-display panel-1col clearfix'})

    bill_name = get_bill_name(source_url, row)
    bill_name = bill_name.upper()
    session = get_session(main_div, row)

    goverlytics_id = f'{prov_terr_abbreviation}_{session}_{bill_name}'
    row.goverlytics_id = goverlytics_id

    # get_bill_type(main_div, row)
    #
    # row.chamber_origin = 'Legislative Assembly'
    #
    # get_bill_title(main_div, row)
    # get_current_status(main_div, row)
    get_actions(main_div, row)

    # chamber_origin = ''
    # bill_type = ''
    # if 'HB' == doc_type:
    #     chamber_origin = 'House'
    #     bill_type = 'Bill'
    # elif ...:
        # Check for other types like SB (senate bills), HRes (House Resolutions), etc.
        # For now we're only work with HB bills so we'll keep it simple





    # The Illinois state legislation website has their data stored in a weird way...
    # everything is stored in spans so we're gonna try pulling the data we need from
    # those. Your implementations will probably look quite a bit different than this.

    # Get bill description and summary
    # bill_description = ''
    # bill_summary = ''
    # spans = soup.findAll('span')
    # for idx, span in enumerate(spans):
    #     txt = span.text
    #     if 'Short Description:' in txt:
    #         bill_description = spans[idx + 1].text
    #     if 'Synopsis As Introduced' in txt:
    #         bill_summary = spans[idx + 1].text.strip()
    # row.bill_description = bill_description
    # row.bill_summary = bill_summary
    #
    # # Get bill sponsors
    # table = soup.find(
    #     'table', {'width': '440', 'border': '0', 'align': 'left'})
    # table_td = table.find('td', {'width': '100%'})
    #
    # a_tag = table_td.findAll('a', href=True)
    # sponsors = []
    # for a in a_tag:
    #     if '/house/Rep.asp' in a['href'] or '/senate/Senator.asp' in a['href']:
    #         sponsors.append(a.text)

    # # We'll now try to get the legislator goverlytics ID. Fortunately for us, this
    # # site provides a unique identifier for each legislator. Normally we would do
    # # the following:
    # sponsor_id = scraper_utils.get_legislator_id(state_member_id=legislator_id)
    # # However, since this is often not the case, we will search for the id using the
    # # legislator name. We are given the legislator's full name, but if you are given
    # # only the legislator initials and last name, which is more often the case, be sure to
    # # use the legislators_search_startswith() method, which might look something like this:
    # sponsor_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', first_initial, name_last=name_last)

    # sponsors_id = []
    # for sponsor in sponsors:
    #     hn = HumanName(sponsor)
    #     name_first = hn.first
    #     name_middle = hn.middle
    #     name_last = hn.last
    #     name_suffix = hn.suffix
    #
    #     search_for = dict(name_first=name_first, name_middle=name_middle,
    #                       name_last=name_last, name_suffix=name_suffix)
    #
    #     sponsor_id = scraper_utils.get_legislator_id(**search_for)
    #
    #     # Some sponsor IDs weren't found, so we won't include these.
    #     # If you are unable to find legislators based on the provided search criteria, be
    #     # sure to investigate. Check the database and make sure things like names match
    #     # exactly, including case and diacritics.
    #     if sponsor_id is not None:
    #         sponsors_id.append(sponsor_id)
    #
    # row.sponsors = sponsors
    # row.sponsors_id = sponsors_id
    #
    # # Get actions
    # actions_table = soup.findAll('table', {
    #                              'width': '600', 'cellspacing': '0', 'cellpadding': '2', 'bordercolor': 'black', 'border': '1'})[1]
    #
    # action_date = ''
    # action_by = ''
    # action_description = ''
    # actions = []
    # number_of_columns = 3
    # # Skip the header row
    # for idx, td in enumerate(actions_table.findAll('td')[3:]):
    #     # With this type of method, normally you would search by 'tr' and then grab the value
    #     # from each 'td' in the row, but for some reason, beautiful soup wasn't able to find
    #     # the 'tr' so I had to get the value using a different, less intuitive method.
    #     mod = idx % number_of_columns
    #     if mod == 0:
    #         action_date = td.text.strip()
    #     if mod == 1:
    #         action_by = td.text.strip()
    #     if mod == 2:
    #         action_description = td.text.strip()
    #         actions.append(
    #             dict(date=action_date, action_by=action_by, description=action_description))
    #
    # # We can get the date introduced from the first action, and the current status from
    # # the most recent action.
    # date_introduced = None
    # current_status = ''
    # if len(actions) > 0:
    #     date_introduced = datetime.datetime.strptime(
    #         actions[0]['date'], '%m/%d/%Y')
    #     current_status = actions[-1]['description']
    #
    # row.actions = actions
    # row.current_status = current_status
    # row.date_introduced = date_introduced

    # There's more data on other pages we can collect, but we have enough data for this demo!

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

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
    # data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database.
    #scraper_utils.write_data(data)

    print('Complete!')
