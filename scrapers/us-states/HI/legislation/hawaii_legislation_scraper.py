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

from legislation_scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
import request_url
import pandas as pd
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import io
import pdfplumber
import urllib.parse as urlparse
from urllib.parse import parse_qs
from pprint import pprint
import datetime
import boto3

# # Initialize config parser and get variables from config file
# configParser = configparser.RawConfigParser()
# configParser.read('config.cfg')

state_abbreviation = 'HI'
database_table_name = 'us_hi_legislation'
legislator_table_name = 'us_hi_legislators'

scraper_utils = USStateLegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)

pdf_url = 'https://www.capitol.hawaii.gov/session2021/bills/'
bill_url = 'capitol.hawaii.gov/measure_indiv.aspx?billtype=HB&billnumber=1000&year=2021'
header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
current_year = '2021'
bill_dict = {
    'DC': {'chamber_origin': '', 'type': ''},
    'GM': {'chamber_origin': '', 'type': ''},
    'HB': {'chamber_origin': 'House', 'type': 'Bill'},
    'HCR': {'chamber_origin': 'House', 'type': 'Concurrent Resolution'},
    'HR': {'chamber_origin': 'House', 'type': 'Resolution'},
    'JC': {'chamber_origin': '', 'type': ''},
    'SB': {'chamber_origin': 'Senate', 'type': 'Bill'}
}


def get_bill_params(url):
    link_lst = []
    url_request = request_url.UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_links = url_soup.find_all('a')
    for item in url_links:
        if '.pdf' in item.get('href') and not re.match('.*HD[0-9].*', item.text):
            test = item.text.replace('_.pdf', '')
            if re.match('[A-Z]{3}', test):
                bill = re.match('[A-Z]{3}', test).group()
            elif re.match('[A-Z]{2}', test):
                bill = re.match('[A-Z]{2}', test).group()
            num = test.replace(bill, '')
            url = 'https://www.capitol.hawaii.gov' + item.get('href')
            link_lst.append({'billtype': bill, 'billnumber': num, 'pdf_link': url})
    return link_lst


def make_bill_urls(lst):
    bill_urls = []
    for item in lst:
        url = 'https://capitol.hawaii.gov/measure_indiv.aspx?billtype=' + item['billtype'] + '&billnumber=' + item[
            'billnumber'] + '&year' + current_year
        bill_info = bill_dict[item['billtype']]
        bill_urls.append({'url': url, 'bill_name': item['billtype'] + item['billnumber'], 'pdf': item['pdf_link'],
                          'bill_info': bill_info})

    print('Done making bill list!')
    return bill_urls


def get_sponsor(soup):
    try:
        sponsor_text = soup.find('span', {'id': 'ctl00_ContentPlaceHolderCol1_ListView1_ctrl0_introducerLabel'}).text
        sponsor_lst = sponsor_text.replace('(Introduced by request of another party)', '').strip().split(',')
        sponsor_lst = [x.strip().title() for x in sponsor_lst]
        if len(sponsor_lst) == 1 and sponsor_lst[0] == '':
            sponsor_lst = []
        return sponsor_lst
    except:
        return []


def change_actions(item):
    if item['action_by'] == 'H':
        item['action_by'] = 'House'
    elif item['action_by'] == 'S':
        item['action_by'] = 'Senate'
    if re.match(r'(([0-9]|[0-9]{2})/){2}[0-9]{4}', item['date']):
        year = item['date'].split('/')
        item['date'] = year[2] + '-' + year[0] + '-' + year[1]
    return item


def get_actions(soup):
    try:
        table = soup.find('table', {'id': 'ctl00_ContentPlaceHolderCol1_GridViewStatus'})
        df = pd.read_html(str(table))[0].rename(
            columns={'Sort by Date': 'date', 'Unnamed: 1': 'action_by', 'Status Text': 'description'})
        json = df.to_dict('records')
        for item in json:
            item = change_actions(item)
        return json
    except:
        return None


def get_billtext(pdf_link):
    try:
        response = requests.get(pdf_link, stream=True)
        pdf = pdfplumber.open(io.BytesIO(response.content))
        bill_txt = ''
        for _ in range(0, len(pdf.pages)):
            page = pdf.pages[_]
            text = ' '.join(page.extract_text().split('\n'))
            bill_txt += text
        return bill_txt
    except:
        return ''


def compare_dates(date1, date2):
    d1 = date1.split('-')
    d2 = date2.split('-')
    t1 = int(d1[1]) * 100 + int(d1[2])
    t2 = int(d2[1]) * 100 + int(d2[2])
    if t1 >= t2:
        return True
    else:
        return False


def get_status(actions):
    current_status = ''
    for item in actions:
        if current_status == '':
            current_status = item
        elif type(current_status) == dict:
            if compare_dates(item['date'], current_status['date']):
                current_status = item

    if type(current_status) == str:
        return current_status
    else:
        return current_status['description']


def get_bill_summary(soup):
    if soup.find('span', {'id': 'ctl00_ContentPlaceHolderCol1_ListView1_ctrl0_descriptionLabel'}):
        return soup.find('span', {'id': 'ctl00_ContentPlaceHolderCol1_ListView1_ctrl0_descriptionLabel'}).text
    else:
        return None


def get_site_topic(soup):
    if soup.find('span', {'id': 'ctl00_ContentPlaceHolderCol1_ListView1_ctrl0_report_titleLabel'}):
        return soup.find('span', {'id': 'ctl00_ContentPlaceHolderCol1_ListView1_ctrl0_report_titleLabel'}).text
    else:
        return None


def scrape(bill_item):
    try:
        row = scraper_utils.initialize_row()

        link = bill_item['url']
        bill_name = bill_item['bill_name']
        pdf = bill_item['pdf']
        bill_info = bill_item['bill_info']

        url_request = request_url.UrlRequest.make_request(link, header)
        soup = BeautifulSoup(url_request.content, 'lxml')

        # Now you can begin collecting data and fill in the row. The row is a dictionary where the
        # keys are the columns in the data dictionary. For instance, we can insert the source_url,
        # like so:

        session = current_year
        actions = get_actions(soup)

        row.session = session
        row.bill_name = bill_name
        sponsors = get_sponsor(soup)
        if len(sponsors) == 1:
            row.principal_sponsor = sponsors
        else:
            row.sponsors = sponsors

        row.bill_summary = get_bill_summary(soup)
        row.source_topic = get_site_topic(soup)
        row.actions = actions
        row.bill_text = get_billtext(pdf)
        row.source_url = link

        goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'
        url = f'us/{state_abbreviation}/legislation/{goverlytics_id}'

        row.goverlytics_id = goverlytics_id
        row.source_url = url

        row.chamber_origin = bill_info['chamber_origin']
        row.bill_type = bill_info['type']

        row.current_status = get_status(actions)

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
            if re.match('[A-Z]\..*', sponsor):
                name_last = sponsor.replace(re.match('[A-Z]\.', sponsor).group(), '').strip().title()
                sponsor_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first',
                                                                         sponsor.split('.')[0], name_last=name_last)
                print(sponsor_id)
            else:
                sponsor_id = scraper_utils.get_legislator_id(name_last=sponsor.title())

            # Some sponsor IDs weren't found, so we won't include these.
            # If you are unable to find legislators based on the provided search criteria, be
            # sure to investigate. Check the database and make sure things like names match
            # exactly, including case and diacritics.
            if sponsor_id is not None:
                sponsors_id.append(sponsor_id)
        if len(sponsors_id) == 1:
            row.principal_sponsor_id = sponsors_id[0]
        elif len(sponsors_id) > 1:
            row.sponsors_id = sponsors_id

        print('Done row for: ' + bill_name)
        return row
    except TypeError:
        return None


if __name__ == '__main__':
    bill_lst = make_bill_urls(get_bill_params(pdf_url))

    with Pool() as pool:
        data = pool.map(scrape, bill_lst)

    print('done scraping!')
    scraper_utils.insert_legislation_data_into_db(data)

    print('Complete!')
