
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

import re
import numpy as np
from nameparser import HumanName
from multiprocessing import Pool
import pandas as pd
from bs4 import BeautifulSoup
import time
from scraper_utils import CAProvTerrLegislatorScraperUtils
from urllib.request import urlopen as uReq
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

prov_abbreviation = 'NS'
database_table_name = 'ca_ns_legislators'

scraper_utils = CAProvTerrLegislatorScraperUtils(
    prov_abbreviation, database_table_name)

base_url = 'https://nslegislature.ca'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():
    urls = []

    path = '/members/profiles'
    scrape_url = base_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    members_view = soup.find('div', {'class': 'view-content'})

    for tr in members_view.findAll('a'):
        a = tr
        urls.append(base_url + a['href'])

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    return urls


def get_current_general_assembly_link(general_assembly_link):

    uClient = uReq(general_assembly_link)
    page_html = uClient.read()
    uClient.close()
    page_soup = BeautifulSoup(page_html, "lxml")
    table = page_soup.find("table", {'class': 'wikitable'})
    current_assembly_row = table.findAll('tr')[1]
    current_assembly = current_assembly_row.findAll('td')[0]
    link = current_assembly.find('a').get('href')
    scraper_utils.crawl_delay(crawl_delay)

    return link


def find_mla_wiki(mlalink):
    bio_links = []
    print(mlalink)
    uClient = uReq(mlalink)
    page_html = uClient.read()
    uClient.close()
    page_soup = BeautifulSoup(page_html, "lxml")
    tables = page_soup.findAll("tbody")
    people = tables[4].findAll("tr")
    for person in people[1:]:
        info = person.findAll("td")
        try:
            biolink = "https://en.wikipedia.org" + (info[2].a["href"])
            print(biolink)
            bio_links.append(biolink)
        except Exception:
            pass

    scraper_utils.crawl_delay(crawl_delay)
    print(bio_links)
    return bio_links


def get_most_recent_term_id(row):
    path = '/members/profiles'
    scrape_url = base_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    assembly = soup.find('h2', {'class': 'paragraph-header'}).text

    scraper_utils.crawl_delay(crawl_delay)
    row.most_recent_term_id = assembly


def get_party(bio_container, row):
    try:
        party = bio_container.find('span', {'class': 'party-name'}).text

        if party == 'PC':
            party = 'Progressive Conservative'
        if party == 'NDP':
            party = 'New Democratic'

        row.party_id = scraper_utils.get_party_id(party)
        row.party = party
    except Exception:
        pass


def get_name(bio_container, row):
    name_full = bio_container.find('div', {'class': 'views-field-field-last-name'}).text.strip()
    name_full = name_full.replace('Honourable', '').strip()
    name_full = name_full.replace('Hon.', '').strip()
    hn = HumanName(name_full)
    row.name_full = name_full

    row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix
    return hn.first + ' ' + hn.last


def get_riding(bio_container, row):
    riding = bio_container.find('td', {'class': 'views-field-field-constituency'}).text.strip()
    riding = riding.replace('\n', '')
    row.riding = riding


def get_phone_number(bio_container, row):
    phone_numbers = []

    phone_detail = bio_container.findAll('dd', {'class': 'numbers'})
    try:
        office_phone = re.findall(r'\(?[0-9]{3}\)?[-, ][0-9]{3}[-, ][0-9]{4}', phone_detail[0].text)[0]
        office_phone = office_phone.replace('(', '').replace(')', '').replace(' ', '-')
        phone = {'office': 'Constituency office', 'number': office_phone}
        phone_numbers.append(phone)
    except Exception:
        pass
    try:
        business_phone = re.findall(r'\(?[0-9]{3}\)?[-, ][0-9]{3}[-, ][0-9]{4}', phone_detail[1].text)[0]
        business_phone = business_phone.replace('(', '').replace(')', '').replace(' ', '-')
        phone_numbers.append({'office': 'Business', 'number': business_phone})
    except Exception:
        pass

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
    try:
        const_office = ','.join(address)
        address = ','.join(address)
    except Exception:
        pass
    const_office = const_office.replace('\n', ', ')
    address = address.replace('\n', ', ')

    c_address = {"location": "Constituency office", "address": const_office}
    b_address = {"location": location, "address": address}
    addresses.append(c_address)
    addresses.append(b_address)

    row.addresses = addresses


def get_email(bio_container, row):
    contact_detail = bio_container.find('dd', {'class': 'numbers'})
    try:
        email = contact_detail.find('a').get('href')
        email = email.split('mailto:')[1]
        row.email = email
    except:
        pass


def get_years_active(bio_container, row):
    time_periods = bio_container.findAll('td', {'class': 'views-field-field-time-period'})
    years_active = []

    for time_period in time_periods:
        if ' - ' not in time_period.text:
            start_year = time_period.text.strip()
            years = 2021 - int(start_year)
        else:
            start_year = time_period.text.split(' - ')[0].strip()
            end_year = time_period.text.split(' - ')[1].strip()
            years = int(end_year) - int(start_year)
        year_counter = int(start_year)
        for i in range(0, years + 1):
            years_active.append(year_counter)
            year_counter += 1
    years_active.sort()
    row.years_active = years_active


def get_committee_role(name, link):
    role = "member"
    page = scraper_utils.request(base_url + link)
    soup = BeautifulSoup(page.content, 'html.parser')
    members = soup.findAll('div', {'class': 'views-row'})
    for member in members:
        if name in member.text:
            try:
                role = member.find('span', {'class': 'mla-committee-title'}).text
            except Exception:
                pass
    scraper_utils.crawl_delay(crawl_delay)
    return role


def get_committees(bio_container, row, name):
    committees = []
    try:
        committee_div = bio_container.find('div', {'class': 'view-committee-listings'})
        committee_list = committee_div.findAll('li')
        for committee in committee_list:
            link = committee.find('a').get('href')
            role = get_committee_role(name, link)
            committee = committee.text.replace('\n', '').strip()
            committee_name = "Standing Committee on " + committee
            committee_detail = {"role": role, "committee": committee_name}
            committees.append(committee_detail)
    except Exception:
        pass
    row.committees = committees


def get_wiki_url(row):
    wiki_base_url = "https://en.wikipedia.org"
    wiki_general_assembly_link = 'https://en.wikipedia.org/wiki/General_Assembly_of_Nova_Scotia'
    uClient = uReq(wiki_base_url + get_current_general_assembly_link(wiki_general_assembly_link))
    page_html = uClient.read()
    uClient.close()

    page_soup = BeautifulSoup(page_html, "html.parser")
    table = page_soup.findAll("table", {'class': 'wikitable'})[0]
    table = table.findAll("tr")[1:]

    for table_row in table:
        tds = table_row.findAll("td")
        name_td = tds[2]
        name = name_td.text
        district = tds[1].text
        
        if row.riding == district.strip() and row.name_last in name.strip():
            row.wiki_url = wiki_base_url + name_td.a['href']
            break


def scrape(url):
    print(url)
    row = scraper_utils.initialize_row()
    row.source_url = url

    region = scraper_utils.get_region(prov_abbreviation)
    row.region = region

    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    bio_container = soup.find('div', {'class': 'panels-flexible-region-mla-profile-current-center'})

    get_most_recent_term_id(row)
    get_party(bio_container, row)
    name = get_name(bio_container, row)
    get_riding(bio_container, row)
    get_phone_number(bio_container, row)
    get_addresses(bio_container, row)
    get_email(bio_container, row)
    get_years_active(bio_container, row)
    get_committees(bio_container, row, name)
    get_wiki_url(row)

    row.gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last, bio_container.text)
    row.role = "Member of the Legislative Assembly"
    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)
    print(row)
    return row


if __name__ == '__main__':
    start = time.time()
    print(
        f'WARNING: This website may take awhile to scrape (about 5-10 minutes using multiprocessing) '
        f'since the crawl delay is very large (ie: {crawl_delay} seconds). '
        f'If you need to abort, press ctrl + c.')
    print('Collecting URLS...')
    urls = get_urls()
    print('URLs Collected.')

    print('Scraping data...')
    data = [scrape(url) for url in urls]


    print(data)
    # with Pool() as pool:
    #     data = pool.map(scrape, urls)
    leg_df = pd.DataFrame(data)

    try:
        leg_df = leg_df.drop(columns="birthday")
        leg_df = leg_df.drop(columns="education")
        leg_df = leg_df.drop(columns="occupation")
        leg_df = leg_df.drop(columns="name_first")
    except:
        pass

    # getting urls from wikipedia
    wiki_general_assembly_link = 'https://en.wikipedia.org/wiki/General_Assembly_of_Nova_Scotia'
    wiki_mla_link = get_current_general_assembly_link(wiki_general_assembly_link)
    mla_wiki = find_mla_wiki('http://en.wikipedia.org' + wiki_mla_link)

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_wiki_bio, mla_wiki)
    wiki_df = pd.DataFrame(wiki_data)[
        ['occupation', 'birthday', 'education', 'name_first', 'name_last', 'wiki_url']]

    wiki_index = wiki_df.index[wiki_df['name_first'] == ''].tolist()
    for index in wiki_index:
        wiki_df = wiki_df.drop(wiki_df.index[index])

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["wiki_url", "name_last"])

    isna = big_df['education'].isna()
    big_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df.loc[isna, 'occupation'] = pd.Series([[]] * isna.sum()).values
    big_df['occupation'] = big_df['occupation'].replace({np.nan: None})

    # dropping rows with vacant seat
    vacant_index = big_df.index[big_df['name_first'] == "Vacant"].tolist()
    for index in vacant_index:
        big_df = big_df.drop(big_df.index[index])

    print('Scraping complete')

    big_list_of_dicts = big_df.to_dict('records')
    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print(f'Scraper ran successfully!')
