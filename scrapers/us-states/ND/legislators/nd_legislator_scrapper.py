'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import re
from datetime import date, datetime
from nameparser import HumanName
from pprint import pprint
import configparser
import sys
import os
sys.path.append("..")
from database import Database
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
from legislator_scraper_utils import LegislatorScraperUtils
from pathlib import Path
# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))


# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = str(configParser.get(
    'scraperConfig', 'state_abbreviation'))
database_table_name = str(configParser.get(
    'scraperConfig', 'database_table_name'))
country = str(configParser.get('scraperConfig', 'country'))

# Initialize database and scraper utils
db_user = str(configParser.get('databaseConfig', 'db_user'))
db_pass = str(configParser.get('databaseConfig', 'db_pass'))
db_host = str(configParser.get('databaseConfig', 'db_host'))
db_name = str(configParser.get('databaseConfig', 'db_name'))

Database.initialise(database=db_name, host=db_host,
                    user=db_user, password=db_pass)

scraper_utils = LegislatorScraperUtils(
    state_abbreviation, database_table_name, country)

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
            fullname +=name_info_dict['middlename'] + ' '
        if name_info_dict['lastname'] != None:
            fullname += name_info_dict['lastname'] + ' '

        fullname = fullname[:len(fullname) - 1]

        if fullname != '':
            name_info_dict['fullname'] = fullname

        return name_info_dict
    except:
        raise
        # sys.exit('error in retrieving name information\n')

def retrieve_current_term():
    date_collected = datetime.now()
    current_year = date_collected.year
    current_term = str(current_year) + '-' + str(current_year + 1)

    return current_term

def calculate_years_active(start_year, end_year):
    years_served = []
    number_of_years = int(end_year) - start_year + 1

    while number_of_years != 0:
        years_served.append(start_year)
        start_year+=1
        number_of_years-=1

    return years_served

def calculate_years_active_format_1(years):
    date_collected = datetime.now()
    current_year = date_collected.year
    start_year = int(re.search(r'(\d{4}$)', years).group(1))
    
    return calculate_years_active(start_year, current_year)

def calculate_years_active_format_2(years):
    year_range = re.search(r'(\d{4}-(?:\d{4}|\d{2})|\d{4})', years).group(1).split('-')
    start_year = int(year_range[0])
    end_year = int(year_range[1])
    # Dont really like this line, checks if the format is 2009-11 and makes it 2009-2011
    if end_year < 100:
        end_year = 2000 + end_year
    return calculate_years_active(start_year, end_year)
    
def retrieve_biography_info(content):
    try:
        biography_items = content.find(
            'div', {'class': 'panel-pane pane-node-body biography'}).find_all('li')
        bio_info_dict = {'occupation': None, 'yearsactive': None}

        if len(biography_items) > 0:
            bio_info_dict['occupation'] = biography_items[0].text.split(';')

        if len(biography_items) > 1:
            years_served_raw = biography_items[len(biography_items) - 1].text.split(',')
            years_active = []
            for ele in years_served_raw:
                # for formats like: since 2005
                if 'since' in ele.lower():
                    years_active.extend(calculate_years_active_format_1(ele))
                # for formats like: 2009-11 or 2009-2011
                elif '-' in ele.lower():
                    years_active.extend(calculate_years_active_format_2(ele))
            bio_info_dict['yearsactive'] = years_active

        return bio_info_dict
    except:
        raise
        # sys.exit('error in retrieving biography information\n')


def retrieve_party(content):
    try:
        string_contaning_party = content.find('div', 'pane-content').text
        legislator_party = None
        for party in POLITICAL_PARTIES:
            if party in string_contaning_party:
                legislator_party = party

        return legislator_party
    except:
        raise
        #  sys.exit('error in retrieving party information\n')



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

def retrieve_address_info(content):
    # address_info_dict = {'areaserved': None,'address': None}
    address = content.find('div', 'adr')
    address_lst = []
    if address != None:
        street = address.find('div', 'street-address').text.strip()
        state = address.find('span', 'locality').text
        region = address.find('span', 'region').text
        postal_code = address.find('span', 'postal-code').text

        complete_address = ''
        if street != None:
            complete_address += street + ' '
        if state != None:
            complete_address += state + ', '
        if region != None:
            complete_address += region + ' '
        if postal_code != None:
            complete_address += postal_code + ' '

        complete_address = complete_address[:len(complete_address) - 1]
        if complete_address != '':
            address_lst.append({'address':complete_address,
                                             'location': 'Capitol Office'})

    return address_lst

def retrieve_area_served(content):
    area_served = content.find('div', 'city').text.split(' ')[1]
    return [area_served]

def retrieve_phone_numbers(content):
    numbers = content.find_all(
            'div', re.compile('panel.*phone.*'))
    phone_number_list = []
    for phone_number in numbers:
        contact_information = {}
        number_type = phone_number.find(
            'div', 'field-label').text.split(':')[0]
        number = phone_number.find('div', 'field-item even').text
        contact_information['number'] = number
        contact_information['type'] = number_type
        phone_number_list.append(contact_information)
    
    return phone_number_list

def retrieve_email(content):
    try:
        email_content = content.find('div', re.compile('panel.*email.*'))
        email = email_content.find('a').text

        return email
    except:
        return None

def retrieve_contact_info(content):
    try:
        contact_dict = {'address': None, 'areaserved': None, 'phonenumbers': None, 'email': None}
        contact_dict['address'] = retrieve_address_info(content)
        contact_dict['areaserved'] = retrieve_area_served(content)
        contact_dict['phonenumbers'] = retrieve_phone_numbers(content)
        contact_dict['email'] = retrieve_email(content)

        return contact_dict
    except:
        raise
        # sys.exit('error in retrieving contact information\n')

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
    
    return urls
    # return ['https://www.legis.nd.gov/assembly/67-2021/members/house/representative-mike-brandenburg']


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

    row = scraper_utils.initialize_row()
    row.state_url = url
    row.most_recent_term_id = retrieve_current_term()

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    name_info_dict = retrieve_name_info(soup)
    # Let scraper_utils.insert_legislator_data_into_db enter default values for empty columns
    if name_info_dict['suffix'] != None:
        row.name_suffix = name_info_dict['suffix']
    if name_info_dict['firstname'] != None:
        row.name_first = name_info_dict['firstname']
    if name_info_dict['middlename'] != None:
        row.name_middle = name_info_dict['middlename']
    if name_info_dict['lastname'] != None:
        row.name_last = name_info_dict['lastname']
    if name_info_dict['fullname'] != None:
        row.name_full = name_info_dict['fullname']
    if name_info_dict['role'] != None:
        row.role = name_info_dict['role']


    content = soup.find('div', {'id': "block-system-main",
                                'class': 'block block-system first last odd'})

    bio_info_dict = retrieve_biography_info(content)
    if bio_info_dict['occupation'] != None:
        row.occupation = bio_info_dict['occupation']
    if bio_info_dict['yearsactive'] != None:
        row.years_active = bio_info_dict['yearsactive']

    row.district = content.find(
        'div', 'pane-content').find('a').text.split(' ')[1]

    party = retrieve_party(content)
    if party != None:
        row.party_id = scraper_utils.get_party_id(party)
        row.party = party

    committees = retrieve_committees(content)
    # If committees is not empty we insert it
    if committees:
        row.committees = committees
    
    contact_info_dict = retrieve_contact_info(content)
    if contact_info_dict['address'] != None:
        row.addresses = contact_info_dict['address']
    if contact_info_dict['areaserved'] != None:
        row.areas_served = contact_info_dict['areaserved']
    if contact_info_dict['phonenumbers'] != None:
        row.phone_number = contact_info_dict['phonenumbers']
    if contact_info_dict['email'] != None:
        row.email = contact_info_dict['email']

    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    try:
        data = [scrape(url) for url in urls]
        with Pool() as pool:
            data = pool.map(scrape, urls)
        # pprint(data)
        # Once we collect the data, we'll write it to the database.
        scraper_utils.insert_legislator_data_into_db(data)
    except:
        sys.exit('error\n')
    print('Complete!')
