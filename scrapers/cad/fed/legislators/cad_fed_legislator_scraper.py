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

def get_mp_basic_details(mp_list_url):
    global df
    page = requests.get(mp_list_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    mp_tiles = soup.find('div', {'id': 'mip-tile-view'})

    mp_data = []
    for tile in mp_tiles.findAll('div', {'class': 'ce-mip-mp-tile-container'}):
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
        province_territory_id = scraper_utils.get_attribute('division', 'division', province_territory)

        role = 'MP'

        mp_data.append(dict(source_id=source_id, source_url=source_url, name_full=name_full,
        name_last=name_last, name_first=name_first, name_middle=name_middle, name_suffix=name_suffix,
        country_id=scraper_utils.country_id, country=scraper_utils.country, party_id=party_id,
        party=party, role=role, riding=riding, province_territory_id=province_territory_id,
        province_territory=province_territory))
    
    df = df.append(mp_data)


def get_mp_fine_details():
    global df
    pass



def scrape():
    mp_list_url = f'{base_url}/members/en/search'
    get_mp_basic_details(mp_list_url)
    get_mp_fine_details()


if __name__ == '__main__':

    scrape()

    print(df.head())
    # with Pool() as pool:
    #     data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database.
    # scraper_utils.insert_legislator_data_into_db(data)

    print('Complete!')
