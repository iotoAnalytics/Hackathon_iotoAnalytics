'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
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
from datetime import date, datetime
from nameparser import HumanName
from pprint import pprint
import configparser

import io
import json
# from PyPDF2 import PdfFileReader
import pdfplumber
# Get path to the root directory so we can import necessary modules


state_abbreviation = 'KY'
database_table_name = 'us_ky_legislation'
legislator_table_name = 'us_ky_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

base_url = 'https://legislature.ky.gov/'
crawl_delay = scraper_utils.get_crawl_delay(base_url)

POLITICAL_PARTIES = ['Republican', 'Democrat',
                     "Libertarian", 'Green', 'Consitituion']


def request_find(base_url, t, att, filter_all=False):
    url_request = requests.get(base_url, verify=False)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    if filter_all:
        return url_soup.find_all(t, att)
    return url_soup.find(t, att)

# Either filters first then returns first "a" tag with href or returns first a tag href found
# Assumes a tag has href


def retrieve_href(base_url, t, att, filter=False, filter_t=None, reg=None):
    content = request_find(base_url, t, att)
    if filter:
        filtered_content = content.find(filter_t, re.compile(reg))
        return filtered_content.a['href']
    return content.a['href']



def find_id(spons):
    sponsors_id = []
    if len(spons) == 0:
        return sponsors_id

    pprint(spons)
    for spon in spons:
            search_for = dict(name_last=spon)
            id = scraper_utils.get_legislator_id(**search_for)
            sponsors_id.append(id)
    return sponsors_id


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:
    senate_members_url = 'https://legislature.ky.gov/Legislators/senate'
    house_members_url = 'https://legislature.ky.gov/Legislators/house-of-representatives'

    # Get url of current year assymbly members
    content = request_find(senate_members_url, 'div', {'id': 'cbqwpctl00_ctl00_m_g_4af53f99_1f77_4ed2_a980_056e3cfc19c5'})
    for link in content.find_all('a'):
        urls.append([link['href'], 'Senate'])

    content = request_find(house_members_url, 'div', {'id': 'cbqwpctl00_ctl00_m_g_4af53f99_1f77_4ed2_a980_056e3cfc19c5'})
    for link in content.find_all('a'):
        urls.append([link['href'], 'House'])
    pprint(urls[0])
    # return [['Legislators/Pages/Legislator-Profile.aspx?DistrictNumber=130', "House"]]
    return urls


def scrape(url):
    '''
    Insert logic here to scrape all URLs acquired in the get_urls() function.

    Do not worry about collecting the goverlytics_id, date_collected, country, country_id,
    state, and state_id values, as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.
    '''
    result = []

    bill_type_dict = {'HB': 'House Bill',
                      'HR': 'House Resolution ',
                      'HCR': 'House Concurrent Resolution',
                      'HJR': 'House Joint Resolution',
                      'SB': 'Senate Bill',
                      'SJR': 'Senate Joint Resolution',
                      'SR': 'Senate Resolution',
                      'SCR': 'Senate Concurrent Resolution'}

    url_request = requests.get(base_url + url[0], verify=False)
    url_soup = BeautifulSoup(url_request.content, 'lxml')

    full_name_unfiltered = url_soup.find('div', {'class': 'row profile-top'}).find('h2').text
    full_name_unfiltered = full_name_unfiltered.split(" ")[1:-1]

    temp = []
    for name in full_name_unfiltered:
        if name != "":
            temp.append(name)

    principal_sponsor = ""
    for index, name in enumerate(temp):
        if len(temp) == 3 and index == 1:
            principal_sponsor = principal_sponsor + name + " "
        else:
            principal_sponsor = principal_sponsor + name + " "

    search_for = dict(name_full=principal_sponsor)
    principal_sponsor_id = scraper_utils.get_legislator_id(**search_for)
    
    url_request = requests.get(url_soup.find('a', {'class': 'block_btn'})['href'], verify=False)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    row = scraper_utils.initialize_row()


    rows = url_soup.find('tbody').find_all('tr')
    bills = []
    
    for row in rows:
        
        bill_name = row.find_all('td')[0].text.replace(" ", "")
        if '*' in bill_name:
            for key in bill_type_dict:
                if key in bill_name:
                    bill_type = bill_type_dict[key]
                    if 'S' in bill_type:
                        chamber_origin = 'Senate'
                    else:
                        chamber_origin = 'House'
            bill_title = row.find_all('td')[1].text

            source_url = row.find('a')['href']
            url_request = requests.get(source_url, verify=False)
            url_soup = BeautifulSoup(url_request.content, 'lxml')
            content = url_soup.find('tbody')
            
            sponsors_id = []
            bill_summary = ''
            for row in content.find_all('tr'):
                target = row.find('th').text
                if 'Sponsor' in target:
                    sponsors = row.find('td').text.replace('\n', "").split(", ")
                    for sponsor in sponsors:
                        sponsors_id.append(sponsor.split(" ")[1])
                    sponsors_id = find_id(sponsors_id)      
                elif 'Summary' in target:
                    bill_summary = row.find('td').text
            bills.append(bill_name)

            row = scraper_utils.initialize_row()
            row.chamber_origin = chamber_origin
            row.principal_sponsor_id = principal_sponsor_id
            row.principal_sponsor = principal_sponsor
            row.sponsors = sponsors
            row.sponsors_id = sponsors_id 
            row.bill_summary = bill_summary
            row.bill_name = bill_name.replace('*', "")
            row.session = '2020-2021'
            row.source_url = source_url
            row.bill_type = bill_type
            row.bill_title = bill_title
            row.goverlytics_id = f'{state_abbreviation}_{row.session}_{row.bill_name}'
            result.append(row)

    return result


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()
    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # try:
    with Pool() as pool:
        result = []
        data = pool.map(scrape, urls)
        for lst in data:
            for row in lst:
                result.append(row)
        # Once we collect the data, we'll write it to the database.
        # scraper_utils.write_data(result)

    # except:
    #     # sys.exit('error\n')
    print('Complete!')
