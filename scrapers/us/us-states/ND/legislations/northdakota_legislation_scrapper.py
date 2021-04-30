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


state_abbreviation = 'ND'
database_table_name = 'us_nd_legislation'
legislator_table_name = 'us_nd_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

base_url = 'https://www.legis.nd.gov/'
crawl_delay = scraper_utils.get_crawl_delay(base_url)

POLITICAL_PARTIES = ['Republican', 'Democrat',
                     "Libertarian", 'Green', 'Consitituion']


def request_find(base_url, t, att, filter_all=False):
    url_request = requests.get(base_url)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
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


def retrieve_name_info(soup):
    try:
        name_content = re.split(
            r'; |, |\*|\n', soup.find('h1', {'class': 'title', 'id': 'page-title'}).text)

        name_info_dict = {'suffix': None, 'firstname': None,
        'middlename': None, 'lastname': None, 'fullname': None, 'role': None}

        if len(name_content) > 1:
            name_info_dict['suffix'] = name_content[1]

        name_content = name_content[0].split(" ")

        for i in range(len(name_content)):
            if i == 0:
                name_info_dict['role'] = name_content[i]
            elif i == 1:
                name_info_dict['firstname'] = name_content[i]
            elif i == 2 and len(name_content) > 3:
                name_info_dict['middlename'] = name_content[i]
            else:
                name_info_dict['lastname'] = name_content[i]

        fullname = ''
        if name_info_dict['suffix'] != None:
            fullname += name_info_dict['suffix'] + ' '
        if name_info_dict['firstname'] != None:
            fullname += name_info_dict['firstname'] + ' '
        if name_info_dict['middlename'] != None:
            fullname += name_info_dict['middlename'] + ' '
        if name_info_dict['lastname'] != None:
            fullname += name_info_dict['lastname'] + ' '

        fullname = fullname[:len(fullname) - 1]

        if fullname != '':
            name_info_dict['fullname'] = fullname

        return name_info_dict
    except:
        raise
        # sys.exit('error in retrieving name information\n')

def retrieve_committees(content):
    try:
        committees_lst = []
        committees = content.find_all('div', 'cmte-item')
        for committee in committees:
            committee_dict = {}
            comittee = committee.text
            result = re.compile(r'(?<=\()(.+?)(?=\))').search(comittee)
            committee_role = 'Member'
            if result != None:
                committee_role = result.group(1)
                committee_name = comittee[:comittee.find('(')].strip()
            else:
                committee_name = comittee
            committee_dict['role'] = committee_role
            committee_dict['committee'] = committee_name
            committees_lst.append(committee_dict)

        return committees_lst
    except:
        raise
        # sys.exit('error in retrieving committees information\n')

def find_id(spons):
    sponsors_id = []
    if len(spons) == 0:
        return sponsors_id
    for spon in spons:
            spon = spon.split()
            if len(spon) > 1:
                search_for = dict(name_middle=spon[0],name_last=spon[1])
            else:
                search_for = dict(name_last=spon[0])
            id = scraper_utils.get_legislator_id(**search_for)
            sponsors_id.append(id)
    return sponsors_id

def retrieve_bill_url(soup):
    result = []
    session = '2020-2021'

    href = soup.find('p', {'class':'sponsor'})
    href = href.find('a')['href']
    page = requests.get(href)
    soup = BeautifulSoup(page.content, 'html.parser')

    content = soup.find('div', {'id':'application'}).find('dl')
    bill_lst = content.find_all('a')
    bill_summary_lst = content.find_all('dd')
    for i in range(len(bill_lst)):
        try:
            result_dict = {}
            bill_name = bill_lst[i].text
            bill_summary = bill_summary_lst[i].text.strip()
            # pprint(f'bill_name: {bill_name}')
            bill_type = None
            chamber_origin = 'House'
            if 'HB' in bill_name: 
                bill_type = 'House Bill'
            elif 'SB' in bill_name:
                bill_type = 'Senate Bill'
                chamber_origin = 'Senate'
            elif 'HCR' in bill_name:
                bill_type = 'House Concurrent Resolution'

            # pprint(f'bill_type: {bill_type}')
            # pprint(f'chamber_origin: {chamber_origin}')

            url = str(bill_lst[i]['href'])
            suffix_url = url.strip('../../')
            base_url = 'https://www.legis.nd.gov/assembly'
            base_url = retrieve_href(
                base_url, 'div', {'class': 'view-content'}, True, 'li', '^first*')

            complete_url = base_url + '/' + suffix_url
            page = requests.get(complete_url)
            soup = BeautifulSoup(page.content, 'html.parser')
            tbl_rows = soup.find('tbody').find_all('tr')
            pdf_suffix_link = tbl_rows[0].find('a')['href'].split('..')[1]
            pdf_link = base_url + pdf_suffix_link    

            # pprint(f'source_url: {pdf_link}')

            response = requests.get(pdf_link, stream=True)
            pdf = pdfplumber.open(io.BytesIO(response.content))
            page = pdf.pages[0]
            text = page.extract_text()
            text = text.split('\n')
            for i in range(len(text)):
                if 'Introduced ' in text[i]:            
                    sponsors = text[i+1].split(', ')
                    
                    principal_sponsor = sponsors[0].split(" ", 1)[1]
                    sponsors = sponsors[1:]
                    # else:
                    #     sponsors = text[6].split(', ')
                    #     principal_sponsor = sponsors[0]

                    if 'Representatives' or 'Representative' or 'Senator' or 'Senators' in text[i+2]:
                        sponsors2 = text[i+2].split(', ')
                        sponsors2[0] = sponsors2[0].split()[1]
                        sponsors = sponsors + sponsors2
            
            # pprint(sponsors)
            # pprint(principal_sponsor)
            pprint(f'source_url: {pdf_link}')
            sponsors_id = find_id(sponsors)
            
            # pprint(f'sponsors_id: {sponsors_id}')
            # pprint(f'sponsors: {sponsors}')
                
            principal_sponsor_id = find_id([principal_sponsor])[0]
            # pprint(f'principal_sponsor_id: {principal_sponsor_id}')
            # pprint(f'principal_sponsor: {principal_sponsor}')
            pdf.close()
            result_dict['bill_name'] = bill_name.replace(" ", "")
            result_dict['source_url'] = pdf_link
            result_dict['chamber_origin'] = chamber_origin
            result_dict['principal_sponsor_id'] = principal_sponsor_id
            result_dict['principal_sponsor'] = principal_sponsor
            result_dict['sponsors'] = sponsors
            result_dict['sponsors_id'] = sponsors_id
            result_dict['bill_summary'] = bill_summary
            result_dict['bill_type'] = bill_type
            result_dict['session'] = session
            result.append(result_dict)
        except:
            continue

    return result    

def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:
    base_url = 'https://www.legis.nd.gov/assembly'

    # Get url of current year assymbly members
    assembly_info_url = retrieve_href(
        base_url, 'div', {'class': 'view-content'}, True, 'li', '^first*')
    assembly_members_url = retrieve_href(assembly_info_url, 'div', {
                                         'class': 'panel-pane pane-custom pane-1'})
    # Retreive href that contain information on each member.
    # Each href contains information on one memnber
    content = request_find(assembly_members_url, 'div',
                           {'class': 'name'}, True)
    for member in content:
        urls.append(member.a['href'])

    # index = 107

    # pprint(urls[index])
    return urls
    # return [urls[index]]


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

    # row.state_url = url
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    # pprint(soup)
    result = []
    bill_dict_list = retrieve_bill_url(soup)

    for obj in bill_dict_list:
        row = scraper_utils.initialize_row()
        row.chamber_origin = obj['chamber_origin'] 
        row.principal_sponsor_id = obj['principal_sponsor_id'] 
        row.principal_sponsor = obj['principal_sponsor'] 
        row.sponsors = obj['sponsors'] 
        row.sponsors_id = obj['sponsors_id'] 
        row.bill_summary = obj['bill_summary']
        row.bill_name = obj['bill_name']
        row.session = obj['session']
        row.source_url = obj['source_url']
        row.bill_type = obj['bill_type']
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
        # pprint(result)
        # Once we collect the data, we'll write it to the database.
        scraper_utils.write_data(result)

    # except:
    #     # sys.exit('error\n')
    print('Complete!')
