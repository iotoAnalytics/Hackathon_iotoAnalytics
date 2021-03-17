'''
Scraper for collecting Canadian federal legislator (ie: MP, or Member of Parliament) data.
Author: Justin Tendeck
Notes:
    Currently, this scraper just collects the most recent data, but it looks like they have
        a swath of historical data we can come back for. It would be especially useful for
        time series analysis. Eg: https://www.ourcommons.ca/members/en/wayne-easter(43)/roles
        (that can be accessed by clicking the link under "All Roles" on an MP's page).
'''

import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

from legislator_scraper_utils import CadFedLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import pandas as pd


scraper_utils = CadFedLegislatorScraperUtils()
base_url = 'https://www.ourcommons.ca'

df = pd.DataFrame()

# Used to swap parties with database representation
party_switcher = {
    'NDP': 'New Democratic',
    'Green Party': 'Green'
}

def get_mp_basic_details():
    global df
    mp_list_url = f'{base_url}/members/en/search'

    page = requests.get(mp_list_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    mp_tiles = soup.find('div', {'id': 'mip-tile-view'})

    mp_data = []
    for tile in mp_tiles.findAll('div', {'class': 'ce-mip-mp-tile-container'})[:15]:
        mp_url = tile.find('a', {'class': 'ce-mip-mp-tile'}).get('href')
        
        source_url = f'{base_url}{mp_url}'
        source_id = mp_url.split('(')[-1][:-1]

        name_suffix = tile.find('div', {'class': 'ce-mip-mp-honourable'}).text.strip()
        name_suffix = 'Hon.' if name_suffix == 'The Honourable' else name_suffix
        name_full = tile.find('div', {'class': 'ce-mip-mp-name'}).text
        hn = HumanName(name_full)
        name_last = hn.last
        name_first = hn.first
        name_middle = hn.middle
        name_suffix = hn.suffix if hn.suffix else name_suffix

        party = tile.find('div', {'class': 'ce-mip-mp-party'}).text
        party = party_switcher[party] if party in party_switcher else party
        party_id = scraper_utils.get_party_id(party)

        riding = tile.find('div', {'class': 'ce-mip-mp-constituency'}).text
        province_territory = tile.find('div', {'class': 'ce-mip-mp-province'}).text
        province_territory = scraper_utils.get_prov_terr_abbrev(province_territory)
        province_territory_id = scraper_utils.get_prov_terr_id(province_territory)
        region = scraper_utils.get_region(province_territory)

        role = 'MP'

        mp_data.append(dict(source_id=source_id, source_url=source_url, name_full=name_full,
        name_last=name_last, name_first=name_first, name_middle=name_middle, name_suffix=name_suffix,
        country_id=scraper_utils.country_id, country=scraper_utils.country, party_id=party_id,
        party=party, role=role, riding=riding, province_territory_id=province_territory_id,
        province_territory=province_territory, region=region))
    
    df = df.append(mp_data)


def get_contact_details(contact_url):
    page = requests.get(contact_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    container = soup.find('div', {'id': 'contact'})

    contact = {'phone_numbers': [], 'addresses': [], 'email': ''}

    # Email found in first p tag of contact container
    email = container.find('p').text.strip()

    hill_container = container.find('div', {'class': 'col-md-3'})
    hill_ptags = hill_container.findAll('p')
    hill_address = hill_ptags[0].get_text(separator=", ").strip().replace('*,', '-').replace(',,', ',')
    hill_phone = hill_ptags[1].get_text(separator=" ").strip().split(' ')[1]

    con_container = container.find('div', {'class': 'ce-mip-contact-constituency-office'})
    con_ptags = con_container.findAll('p')
    con_address = con_ptags[0].get_text()
    # con_phone = con_ptags[1].get_text(separator=" ").strip().split(' ')[1]

    print(con_address)
    


def get_mp_fine_details():
    global df

    df = df.head(1)

    for i, row in df.iterrows():
        contact_url = f"{row['source_url']}#contact"
        contact = get_contact_details(contact_url)


    



def scrape():
    get_mp_basic_details()
    get_mp_fine_details()


if __name__ == '__main__':

    scrape()

    # with Pool() as pool:
    #     data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database.
    # scraper_utils.insert_legislator_data_into_db(data)

    print('Complete!')
