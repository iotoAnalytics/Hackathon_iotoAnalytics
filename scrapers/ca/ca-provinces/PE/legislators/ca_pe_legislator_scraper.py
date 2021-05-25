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
import re
import numpy as np
from nameparser import HumanName
from pprint import pprint
from multiprocessing import Pool
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from scraper_utils import CAProvTerrLegislatorScraperUtils
from urllib.request import urlopen as uReq
from datetime import datetime

prov_abbreviation = 'PE'
database_table_name = 'ca_pe_legislators'

scraper_utils = CAProvTerrLegislatorScraperUtils(
    prov_abbreviation, database_table_name)

base_url = 'https://www.assembly.pe.ca'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Url we are scraping: https://nslegislature.ca/members/profiles
    path = '/members'
    scrape_url = base_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    members_list = soup.find_all('span', {'class': 'member-title'})

    # We'll collect only the first 10 to keep things simple. Need to skip first record
    for member in members_list:
        a = member.find('a').get('href')
        urls.append(base_url + a)

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)
    return urls


def get_current_general_assembly_link(general_assembly_link):
    uClient = uReq(general_assembly_link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = BeautifulSoup(page_html, "lxml")
    table = page_soup.find("table", {'class': 'wikitable'})
    current_assembly_row = table.findAll('tr')[1]
    current_assembly = current_assembly_row.findAll('td')[0]
    link = current_assembly.find('a').get('href')

    scraper_utils.crawl_delay(crawl_delay)
    return link


def find_mla_wiki(mlalink):
    bio_links = []
    uClient = uReq(mlalink)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = BeautifulSoup(page_html, "lxml")
    tables = page_soup.findAll("tbody")
    people = tables[1].findAll("tr")
    for person in people[1:]:
        info = person.findAll("td")
        try:
            biolink = "https://en.wikipedia.org" + (info[2].a["href"])
            bio_links.append(biolink)
        except Exception:
            pass

    scraper_utils.crawl_delay(crawl_delay)
    return bio_links


def get_party(bio_container, row):
    party = bio_container.find('div', {'class': 'views-field-field-member-pol-affiliation'}).text.strip()
    try:

        if party == 'PC':
            party = 'Progressive Conservative'
        if party == 'NDP':
            party = 'New Democratic'

        row.party_id = scraper_utils.get_party_id(party)
        row.party = party
    except Exception:
        pass


def get_name(bio_container, row):
    name_full = bio_container.find('span', {'class': 'field--name-title'}).text

    hn = HumanName(name_full)
    row.name_full = name_full
    row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix


def get_riding(bio_container, row):
    riding = bio_container.find('div', {'class': 'views-field-field-member-constituency'}).text.strip()
    row.riding = riding


def get_phone_number(bio_container, row):
    phone_numbers = []

    phone_detail = bio_container.find('div', {'class': 'field--name-field-member-contact-information'})

    if "To contact" in phone_detail.text:
        try:
            location_one = phone_detail.findAll('p')[0].text
            location_one = location_one[location_one.index("Minister"): location_one.index(":")]
            location_two = "MLA Office"
        except Exception:
            pass
        try:
            office_phone = re.findall(r'\(?[0-9]{3}\)?[-, ][0-9]{3}[-, ][0-9]{4}', phone_detail.text)[0]
            phone = {'office': location_one, 'number': office_phone}
            phone_numbers.append(phone)
        except Exception:
            pass
        try:
            business_phone = re.findall(r'\(?[0-9]{3}\)?[-, ][0-9]{3}[-, ][0-9]{4}', phone_detail.text)[2]
            phone_numbers.append({'office': location_two, 'number': business_phone})
        except Exception:
            pass
    else:
        office_phone = re.findall(r'\(?[0-9]{3}\)?[-, ][0-9]{3}[-, ][0-9]{4}', phone_detail.text)[0]
        phone = {'office': "office", 'number': office_phone}
        phone_numbers.append(phone)

    row.phone_numbers = phone_numbers


def get_addresses(bio_container, row):
    addresses = []
    contact = bio_container.find('div', {'class': 'mla-current-profile-contact'})
    address_details = contact.findAll('p')
    const_office = address_details[0].text
    const_office = const_office.split('address:')[1]
    bus_office = address_details[2].text
    if re.findall(r'\(?[0-9]{3}\)?[-, ][0-9]{3}[-, ][0-9]{4}', bus_office):
        bus_office = address_details[3].text
    bus_add = bus_office.split('\n')
    address = bus_add[1:]
    location = bus_add[0]

    c_address = {"location": "Constituency office", "address": const_office}
    b_address = {"location": location, "address": address}
    addresses.append(c_address)
    addresses.append(b_address)

    print(addresses)
    row.addresses = addresses


def get_email(bio_container, row):
    contact_detail = bio_container.find('div', {'class': 'field--name-field-member-contact-information'})
    email = contact_detail.find('a').get('href')
    email = email.split('mailto:')[1]
    if "assembly" in email:
        row.email = email


def get_most_recent_term_id(years_active, row):
    year = years_active[-1]
    row.most_recent_term_id = year


def get_years_active(bio_container, row):
    table = bio_container.find('div', {'class': 'view-member-history-table'})
    table_body = table.find('tbody')
    years_active = []
    table_rows = table_body.findAll('tr')
    for tr in table_rows:
        start_date = tr.findAll('td')[2].text
        start_year = re.findall(r'[0-9]{4}', start_date)[0]
        end_date = tr.findAll('td')[3].text
        if "Current" in end_date:
            end_year = datetime.now().year
        else:
            end_year = re.findall(r'[0-9]{4}', end_date)[0]

        years = int(end_year) - int(start_year)
        year_counter = int(start_year)
        for i in range(0, years + 1):
            years_active.append(year_counter)
            year_counter += 1
    years_active.sort()
    years_active = list(dict.fromkeys(years_active))
    get_most_recent_term_id(years_active, row)
    row.years_active = years_active


def get_committees(bio_container, row):
    committees = []
    try:
        committee_div = bio_container.find('div', {'class': 'view-member-committees-table'})
        committee_table = committee_div.find('tbody')
        committee_list = committee_table.findAll('tr')
        for committee in committee_list:
            committee_name = committee.findAll('td')[0].text.strip()
            role = committee.findAll('td')[1].text.strip()
            committee_detail = {"role": role, "committee": committee_name}
            committees.append(committee_detail)
    except Exception:
        pass
    row.committees = committees


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
    # keys are the columns in the data dictionary. For instance, we can insert the state_url
    # like so:
    row.source_url = url

    # get region
    region = scraper_utils.get_region(prov_abbreviation)
    row.region = region

    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    bio_container = soup.find('section', {'class': 'section'})

    get_party(bio_container, row)
    get_name(bio_container, row)
    get_riding(bio_container, row)
    get_phone_number(bio_container, row)
    #get_addresses(bio_container, row)
    get_email(bio_container, row)
    get_years_active(bio_container, row)
    get_committees(bio_container, row)

    row.role = "Member of the Legislative Assembly"
    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    start = time.time()
    print(
        f'WARNING: This website may take awhile to scrape (about 5-10 minutes using multiprocessing) '
        f'since the crawl delay is very large (ie: {crawl_delay} seconds). '
        f'If you need to abort, press ctrl + c.')
    print('Collecting URLS...')
    urls = get_urls()
    print('URLs Collected.')

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:

    print('Scraping data...')

    # data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, urls)
    leg_df = pd.DataFrame(data)
    # leg_df = leg_df.drop(columns="birthday")
    # leg_df = leg_df.drop(columns="education")
    # leg_df = leg_df.drop(columns="occupation")
    # # dropping rows with vacant seat
    # vacant_index = leg_df.index[leg_df['name_first'] == "Vacant"].tolist()
    # for index in vacant_index:
    #     leg_df = leg_df.drop(index)
    #
    # # getting urls from wikipedia
    # wiki_general_assembly_link = 'https://en.wikipedia.org/wiki/Legislative_Assembly_of_Prince_Edward_Island'
    # mla_wiki = find_mla_wiki('https://en.wikipedia.org' + wiki_general_assembly_link)
    #
    # with Pool() as pool:
    #     wiki_data = pool.map(scraper_utils.scrape_wiki_bio, mla_wiki)
    # wiki_df = pd.DataFrame(wiki_data)[
    #     ['occupation', 'birthday', 'education', 'name_first', 'name_last']]
    #
    # big_df = pd.merge(leg_df, wiki_df, how='left',
    #                   on=["name_first", "name_last"])
    #
    # isna = big_df['education'].isna()
    # big_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values
    # big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    # big_df.loc[isna, 'occupation'] = pd.Series([[]] * isna.sum()).values
    # big_df['occupation'] = big_df['occupation'].replace({np.nan: None})

    print('Scraping complete')

    big_list_of_dicts = big_df.to_dict('records')
    print('Writing data to database...')

    #scraper_utils.write_data(big_list_of_dicts)

    print(f'Scraper ran successfully!')
