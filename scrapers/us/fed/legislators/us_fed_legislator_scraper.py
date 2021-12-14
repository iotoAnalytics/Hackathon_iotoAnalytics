import multiprocessing
import os
import re
import sys

from enum import Enum
from multiprocessing import Pool
from pathlib import Path

NODES_TO_ROOT = 4
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import numpy as np
import pandas as pd

from bs4 import BeautifulSoup as soup
from nameparser import HumanName
from scraper_utils import USFedLegislatorScraperUtils
from urllib.request import urlopen

BALLOTPEDIA_BASE_URL = "https://ballotpedia.org"
BALLOTPEDIA_REP_URL = BALLOTPEDIA_BASE_URL + "/List_of_current_members_of_the_U.S._Congress"
HOUSE_BASE_URL = "https://www.house.gov"
HOUSE_REP_URL = HOUSE_BASE_URL + "/representatives"
SENATE_BASE_URL = "https://www.senate.gov"
SENATE_REP_URL = SENATE_BASE_URL + "/"

COLUMNS_NOT_ON_MAIN_SITE = ['birthday', 'education', 'occupation', 'years_active']
NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)

scraper_utils = USFedLegislatorScraperUtils()
house_crawl_delay = scraper_utils.get_crawl_delay(HOUSE_BASE_URL)
senate_crawl_delay = scraper_utils.get_crawl_delay(SENATE_BASE_URL)

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

class RepType(Enum):
    HOUSE = 0
    SENATE = 1

class HouseRepresentative:
    def __init__(self, info: dict):
        self.rep_info = info
        self.row = scraper_utils.initialize_row()

    def collect_data(self):
        self.row.source_url = self.rep_info.get("url")
        self.row.state = self.rep_info.get("state")
        self.row.state_id = scraper_utils.get_attribute('division', 'division', self.row.state)
        self.row.role = "Representative"

        self.row.district = self._extract_district_num()
        self.row.wiki_url = Utils().get_wiki_url(RepType.HOUSE, self.row)

        self._set_name_data()
        self._set_party_data()

    def get_data(self):
        return self.row

    def _extract_district_num(self):
        district = self.rep_info.get("district")
        try:
            return re.match(r'[0-9]+', district).group(0)
        except:
            return district

    def _set_name_data(self):
        name_full = HumanName(self.rep_info.get("name"))
        self.row.name_full = name_full.full_name
        self.row.name_last = name_full.last
        self.row.name_first = name_full.first
        self.row.name_middle = name_full.middle
        self.row.name_suffix = name_full.suffix

    def _set_party_data(self):
        party = self.rep_info.get("party")
        if party == "D":
            self.row.party = "Democrat"
        if party == "R":
            self.row.party = "Republican"

        self.row.party_id = scraper_utils.get_party_id(self.row.party)

class Senate:
    pass

class Utils:
    def get_page_as_soup(self, url):
        page = urlopen(url)
        scraper_utils.crawl_delay(house_crawl_delay)
        return soup(page, "html.parser")

    def pool_data_collection(self, function, iterable):
        data = []
        with Pool(processes=NUM_POOL_PROCESSES) as pool:
            data = pool.map(function, iterable)
        return data

    def get_house_data(self, info):
        house_rep = HouseRepresentative(info)
        house_rep.collect_data()
        return house_rep.get_data()

    def get_all_wiki_urls(self):
        page_soup = self.get_page_as_soup(BALLOTPEDIA_REP_URL)
        tables = page_soup.findAll("table", {"id":"officeholder-table"})
        urls = []
        for table in tables:
            tbody = table.find("tbody")
            trs = tbody.findAll("tr")
            for tr in trs:
                try:
                    tds = tr.findAll("td")
                    urls.append(tds[1].a["href"])
                except:
                    pass
        return urls

    def configure_data(self, mla_data, wiki_data):
        mla_df = pd.DataFrame(mla_data)
        mla_df = mla_df.drop(columns = COLUMNS_NOT_ON_MAIN_SITE)
        
        wiki_df = pd.DataFrame(wiki_data)[
            ['birthday', 'education', 'wiki_url', 'occupation', 'years_active']
        ]

        mla_wiki_df = pd.merge(mla_df, wiki_df, 
                            how='left',
                            on=['wiki_url'])
        mla_wiki_df['birthday'] = mla_wiki_df['birthday'].replace({np.nan: None})
        mla_wiki_df['occupation'] = mla_wiki_df['occupation'].replace({np.nan: None})
        mla_wiki_df['education'] = mla_wiki_df['education'].replace({np.nan: None})
        mla_wiki_df['years_active'] = mla_wiki_df['years_active'].replace({np.nan: None})

        return mla_wiki_df.to_dict('records')

    def get_wiki_url(self, type: RepType, row):
        page_soup = self.get_page_as_soup(BALLOTPEDIA_REP_URL)
        rep_tables = page_soup.findAll("table", {"id":"officeholder-table"})

        if type.name == "HOUSE":
            table = rep_tables[1]
            trs = table.findAll("tr")
            rep_row = self._find_house_rep_row(trs, row)

        if type.name == "SENATE":
            table = rep_tables[0]

        name_position = 1
        return rep_row.findAll("td")[name_position].a["href"]

    def _find_house_rep_row(self, trs, row):
        district = row.district
        if not district.isdigit():
            district = ''

        for tr in trs:
            text = tr.text
            if row.state in text and district in text:
                return tr

    def get_legislator_info(self, type: RepType):
        rep_info_list = []

        if type.name == "HOUSE":
            page_soup = self.get_page_as_soup(HOUSE_REP_URL)
            main_content = page_soup.find("div", {"class":"view-content"})

            state_tables = main_content.findAll("table")
            for state in state_tables:
                rep_info_list.extend(self._extract_state_info(state))
        
        if type.name =="SENATE":
            pass

        return rep_info_list

    def _extract_state_info(self, state):
        state_info = []
        state_name = state.find("caption").text.strip()
        trs = state.find("tbody").findAll("tr")
        for legislator in trs:
            state_info.append(self._get_house_rep_info(state_name, legislator))
        return state_info

    def _get_house_rep_info(self, state_name, legislator_row):
        td_positions = {
            "district": 0,
            "name": 1,
            "party": 2,
            "phone": 4,            
        }
        tds = legislator_row.findAll("td")

        info = {"state":state_name}

        for column in td_positions.keys():
            info[column] = tds[td_positions.get(column)].text.strip()
        
        info["url"] = tds[td_positions.get("name")].a["href"]

        return info

if __name__ == "__main__":
    house_rep_info = Utils().get_legislator_info(RepType.HOUSE)
    house_data = Utils().pool_data_collection(Utils().get_house_data, house_rep_info[:10])
    print(type(house_data))
    
    # wiki_urls = Utils().get_all_wiki_urls()
    # wiki_data = Utils().pool_data_collection(scraper_utils.scrape_wiki_bio, house_data)

    # data = Utils().configure_data(house_data, wiki_data)
    # print(data)

'''
Problem with getting address:
- Each legislator has a unique page
- Sometimes the address is surrounded by an address block
- Often times, it's not

- Should do a find function using some filters for if it is a header, if the header has office in it, etc.

address_regex = r'[0-9]+ [\w\d\s]+,[\w\d ]+'
phone_regex = r'\(?[0-9]{3}\)?-[0-9]{3}-[0-9]{4}'
'''