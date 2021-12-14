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

from legislator_scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = 'LA'
database_table_name = 'us_la_legislators_test'
country = 'US'

scraper_utils = USStateLegislatorScraperUtils(state_abbreviation, database_table_name)

def get_urls():
    """
    collect all urls of legislator's personal page
    """
    urls = []
    # Logic goes here! Some sample code:
    base_url = 'https://house.louisiana.gov'
    path = '/H_Reps/H_Reps_FullInfo'
    scrape_url = base_url + path

    # request and soup
    page = requests.get(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    # urls = {base_url + prod_path['href'] for prod_path in soup.findAll('a', {'href': re.compile("H_Reps/members")})}

    legislator_list = soup.find_all('div', {'class': 'media-body'})
    for item in legislator_list:
        name = item.find('span', {'id': re.compile("body_ListView1_LASTFIRSTLabel")}).text
        if "Vacant" not in name:
            # leave out the vacant seat
            path = item.find('a', {'href': re.compile("H_Reps/members")})
            scrape_url = base_url + path['href']
            urls.append(scrape_url)

    return urls


def get_legislator_info_from_main_page(soup):
    """
    get name, district, party, address, number and email of legislators from main page.
    """
    info_list = []
    name_first = []
    name_middle = []
    # name_last = []
    legislator_list = soup.find_all('div', {'class': 'media-body'})

    for item in legislator_list:
        name = item.find('span', {'id': re.compile("body_ListView1_LASTFIRSTLabel")}).text
        if "Vacant" not in name:
            name_full = name
            name_parser = HumanName(name)
            name_last = name_parser.last
            name_first = name_parser.first
            name_middle = name_parser.middle
            district = item.find('span', {'id': re.compile("body_ListView1_DISTRICTNUMBERLabel")}).text
            party = item.find('span', {'id': re.compile("body_ListView1_PARTYAFFILIATIONLabel")}).text
            address = item.find('span', {'id': re.compile("body_ListView1_OFFICEADDRESSLabel")}).text
            phone_number = item.find('span', {'id': re.compile("body_ListView1_DISTRICTOFFICEPHONELabel")}).text
            email = item.find('span', {'id': re.compile("body_ListView1_EMAILADDRESSPUBLICLabel")}).text

            info_dict = {
                'name_full': name_full,
                'name_last': name_last,
                'name_first': name_first,
                'name_middle': name_middle,
                'district': district,
                'party': party,
                'address': address,
                'phone_number': phone_number,
                'email': email
            }

            info_list.append(info_dict)

    return info_list



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

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:
    row.source_url = url

    # The only thing to be wary of is collecting the party and party_id. You'll first have to collect
    # the party name from the website, then get the party_id from scraper_utils
    # This can be done like so:

    # Replace with your logic to collect party for legislator.
    # Must be full party name. Ie: Democrat, Republican, etc.

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    # Get the crawl delay: AttributeError: 'USStateLegislatorScraperUtils' object has no attribute 'get_crawl_delay'
    # crawl_delay = scraper_utils.get_crawl_delay(url)
    # scraper_utils.crawl_delay(crawl_delay)

    # name
    name_full = soup.find('span', {'id': re.compile("body_FormView5_FULLNAMELabel")}).text
    hn = HumanName(name_full)
    row.name_full = name_full
    row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix

    # party
    party = soup.find('span', {'id': re.compile("body_FormView5_PARTYAFFILIATIONLabel")}).text
    row.party_id = scraper_utils.get_party_id(party)
    row.party = party

    # district
    district = soup.find('span', {'id': re.compile("body_FormView5_DISTRICTNUMBERLabel")}).text
    row.district = district

    # address
    addresses = soup.find('span', {'id': re.compile("body_FormView3_OFFICEADDRESS2Label")}).text
    addresses = [{'location': 'district office', 'address': addresses}]
    row.addresses = addresses


    # phone number
    phone_number = soup.find('span', {'id': re.compile("body_FormView3_DISTRICTOFFICEPHONELabel")}).text
    phone_number = [{'office': 'district office', 'number': phone_number}]
    row.phone_number = phone_number

    # email
    email = soup.find('span', {'id': re.compile("body_FormView6_EMAILADDRESSPUBLICLabel")}).text
    row.email = email

    # committees
    committees = []
    committees_html = soup.find('span', {'id': re.compile("body_FormView1_COMMITTEEASSIGNMENTS2Label")})
    for committee in committees_html.stripped_strings:
        if 'Chairman' in committee:
            committee_name = committee.replace(', Chairman', '')
            committee_dict = {'role': 'chairman', 'committee': committee_name.lower()}
            committees.append(committee_dict)
        elif 'Vice Chair' in committee:
            committee_name = committee.replace(', Vice Chair', '')
            committee_dict = {'role': 'vice chair', 'committee': committee_name.lower()}
            committees.append(committee_dict)
        else:
            committee_dict = {'role': 'member', 'committee': committee.lower()}
            committees.append(committee_dict)

    row.committees = committees


    # year_active: I have year_elected on the webpages, should I rely on that info alone and convert that into
    # years_active? or should I collect that elsewhere?

    # education: the webpage also have edu info but they are unstructured and unable to meet the dataDict requirements

    # Other than that, you can replace this statement with the rest of your scraper logic.
    print(row)
    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    # html = get_main_page_html()
    # info_list = get_legislator_info_from_main_page(html)
    # pprint(info_list)

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape(url) for url in urls]
    # with Pool() as pool:
    #     data = pool.map(scrape, urls)
    #
    # # Once we collect the data, we'll write it to the database.
    # scraper_utils.insert_legislator_data_into_db(data)
    urls = get_urls()
    data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, urls)


    print('Complete!')
