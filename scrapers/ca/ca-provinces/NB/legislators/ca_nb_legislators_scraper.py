import sys
import os
from pathlib import Path

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from bs4 import BeautifulSoup as soup
from multiprocessing import Pool
from nameparser import HumanName as hn
from scraper_utils import CAProvTerrLegislatorScraperUtils
from urllib.request import urlopen

import pandas as pd
import re
import time
import numpy as np

BASE_GOV_WEBSITE = 'https://www.gnb.ca'
GOV_SITE_WITH_LINK_TO_MLAS = BASE_GOV_WEBSITE + '/legis/index-e.asp'
MLA_CONTACT_BASE_URL = 'https://www2.gnb.ca/content/gnb/en/contacts'
MLA_REPORT_URL = MLA_CONTACT_BASE_URL + '/MLAReport.html'
THREADS_FOR_POOL = 12
WIKI_BASE_URL = 'https://en.wikipedia.org'
WIKI_URL = WIKI_BASE_URL + '/wiki/Legislative_Assembly_of_New_Brunswick'

scraper_utils = CAProvTerrLegislatorScraperUtils('NB', 'ca_nb_legislators')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

COLUMNS_NOT_ON_MAIN_SITE = ["years_active", "birthday", "occupation", "education", "wiki_url"]

def program_driver():
    utils = Utils()
    mla_biography_links_by_district_id = utils.get_mla_bio_links()
    mla_contact_info_links = utils.get_mla_contacts_link()
    legislators = []
    for key, value in mla_biography_links_by_district_id.items():
        name_and_party = value.get('name_and_party')
        district = value.get('district')
        bio_url = value.get('bio_url')
        email = value.get('email')
        contacts_url = mla_contact_info_links.get(key)
        legislators.append(Legislator(name_and_party, contacts_url, bio_url, district, email))
    
    mla_data = utils.get_data_from_all_iterable(utils.set_and_get_legislator_data, legislators)
    
    wiki_urls = utils.get_wiki_urls()
    wiki_data = utils.get_data_from_all_iterable(scraper_utils.scrape_wiki_bio, wiki_urls)

    complete_data_set = utils.configure_data(mla_data, wiki_data)
    print("Writing to database...")
    scraper_utils.write_data(complete_data_set)

    # TODO make committee class and get committee information
    
class Legislator:
    def __init__(self, name_and_party: str, contact_info_url, bio_url, district, email):
        self.name_and_party = name_and_party
        self.contact_info_url = contact_info_url
        self.bio_url = bio_url
        self.district = district
        self.email = email
        self.row = scraper_utils.initialize_row()
        
    def set_data(self):
        self.row.source_id = self.__get_source_id()
        self.row.most_recent_term_id = self.__get_most_recent_term_id()
        self.row.source_url = self.contact_info_url
        self.set_name_data()
        self.row.party = self.__get_party()
        self.row.party_id = scraper_utils.get_party_id(self.row.party)
        self.row.role = "Member"
        self.row.riding = self.district
        self.row.phone_numbers = self.__get_phone_numbers()
        self.row.addresses = self.__get_addresses()
        self.row.email = self.email.strip()
        self.row.gender = self.__get_legislator_gender()

    def __get_source_id(self):
        source_id = self.contact_info_url.split('renderer.')[1]
        source_id = re.match('[0-9]+', source_id).group()
        return source_id
        
    def __get_most_recent_term_id(self):
        term_id = self.bio_url.split('bios/')[1]
        term_id = re.match(r'[0-9]+', term_id).group()
        return term_id

    def set_name_data(self):
        name_full = self.__get_mla_name()
        human_name = hn(name_full)
        self.row.name_full = human_name.full_name
        self.row.name_last = human_name.last
        self.row.name_first = human_name.first
        self.row.name_middle = human_name.middle
        self.row.name_suffix = human_name.suffix

    def __get_mla_name(self):
        name = re.split(r'\([A-Z]+\)$', self.name_and_party)[0].strip()
        name = re.sub(r'\([A-Z]+\)', '', name).replace('  ', ' ').replace('Hon. ', '')
        return name.strip()

    def get_data(self):
        return self.row

    def __get_party(self):
        party = re.search(r'\([A-Z]+\)$', self.name_and_party)[0].strip()
        party = party.split('(')[1].split(')')[0].strip()
        parties = {
            'PC': 'Progressive Conservative',
            'L': 'Liberal',
            'G': 'Green',
            'PA': 'People\'s Alliance',
            'ND': 'New Democratic'
        }
        return parties[party]

    def __get_phone_numbers(self):
        contact_div = self.__get_contact_div()
        contact_info = contact_div.find_all('div', {'class':'col-sm-6'})[0].text
        number = contact_info.split(':')[1].split('Fax')[0].strip()
        number = number.replace('(', '').replace(')', '').replace('  ', ' ').replace(' ', '-')
        return [{'number':number, 'office':'office'}]

    def __get_addresses(self):
        contact_div = self.__get_contact_div()
        contact_info = contact_div.find_all('div', {'class':'col-sm-6'})[1]
        mailing_address = repr(contact_info.find('p'))
        mailing_address = mailing_address.replace('<p class="reset">', '').replace('</p>', '').replace('<br>', ' ').replace('<br/>', ' ').strip()
        return [{"address":mailing_address, "location":"Office"}]
    
    def __get_contact_div(self):
        page_soup = Utils().get_page_as_soup(self.contact_info_url)
        return page_soup.find('div', {'class':'item_services'})

    def __get_legislator_gender(self):
        page_soup = Utils().get_page_as_soup(self.bio_url)
        bio_container = page_soup.find('span', {'class':'text1'})
        bio = bio_container.text

        return scraper_utils.get_legislator_gender(self.row.name_first, self.row.name_last, bio)

class Utils:
    def get_page_as_soup(self, url):
        page_html = self.__get_site_as_html(url)
        return soup(page_html, 'html.parser')

    def __get_site_as_html(self, url):
        uClient = urlopen(url)
        page_html = uClient.read()
        uClient.close()

        crawl_delay = scraper_utils.get_crawl_delay(url)
        scraper_utils.crawl_delay(crawl_delay)
        return page_html

    def get_mla_bio_links(self):
        link_to_current_MLAs = self.__get_link_to_current_MLAs()
        base_url_to_current_MLAs = link_to_current_MLAs.split('/index-e.asp')[0]
        
        page_soup = self.get_page_as_soup(link_to_current_MLAs)
        table = page_soup.find('table', {'id' : 'example'})
        trs = table.tbody.find_all('tr')

        mla_bio_links = {}
        for tr in trs:
            name_column = tr.find_all('td')[2]
            if 'Vacant' not in name_column.text:
                link = name_column.a["href"]
                link = base_url_to_current_MLAs + '/' + link.split('./')[1]
                district_id = int(tr.find_all('td')[0].text)
                district = tr.find_all('td')[1].text
                email = tr.find_all('td')[3].text

                mla_bio_links[district_id] = {"name_and_party" : name_column.text, "bio_url" : link, "district" : district, "email" : email}
        return mla_bio_links

    def __get_link_to_current_MLAs(self):
        page_soup = self.get_page_as_soup(GOV_SITE_WITH_LINK_TO_MLAS)
        return page_soup.find_all(self.__is_a_tag_and_has_text_MLAs)[0]["href"]

    def __is_a_tag_and_has_text_MLAs(self, tag):
        return tag.has_attr("href") and tag.text == 'MLAs'

    def get_mla_contacts_link(self):
        page_soup = self.get_page_as_soup(MLA_REPORT_URL)
        trs = page_soup.find('table').find_all('tr')
        mla_district_and_contact_url = {}
        for tr in trs:
            if 'Electoral District' in tr.text:
                district_id = self.__get_district_id(tr.find_all('td')[0].text)
                link = self.__get_mla_contact_link(tr.find_all('td')[1])
                mla_district_and_contact_url[district_id] = link
        return mla_district_and_contact_url

    def __get_district_id(self, text:str):
        id = text.split('Electoral District')[1].strip()
        return int(id)

    def __get_mla_contact_link(self, td_element):
        url = td_element.find('a')['href']
        return MLA_CONTACT_BASE_URL + '/' + url

    def get_wiki_urls(self):
        page_soup = self.get_page_as_soup(WIKI_URL)
        table = page_soup.find("table", {"class": "wikitable sortable"}).find('tbody')
        trs = table.findAll("tr")[1:]
        urls = []
        for tr in trs:
            try:
                td = tr.findAll("td")[1]
                url = WIKI_BASE_URL + td.a["href"]
                urls.append(url)
            except:
                if "Vacant" in tr.findAll("td")[1].text:
                    district = tr.findAll("td")[3].text
                    print(f"Vacant seat in region {district}")
                else:
                    print(f"Error finding link for region {district}")
        return urls

    def get_data_from_all_iterable(self, function, iterable):
        data = []
        with Pool(THREADS_FOR_POOL) as pool:
            data = pool.map(func=function,
                            iterable=iterable)
        return data

    def set_and_get_legislator_data(self, legislator: Legislator):
        legislator.set_data()
        return legislator.get_data()

    def configure_data(self, mla_data, wiki_data):
        mla_df = pd.DataFrame(mla_data)
        mla_df = mla_df.drop(columns = COLUMNS_NOT_ON_MAIN_SITE)

        wiki_df = pd.DataFrame(wiki_data)[[
            "name_last", "name_first", "birthday", "years_active", "wiki_url", "occupation", "education"
        ]]

        mla_wiki_df = pd.merge(mla_df, wiki_df, 
                            how='left',
                            on=['name_first', 'name_last'])
        mla_wiki_df['birthday'] = mla_wiki_df['birthday'].replace({np.nan: None})
        mla_wiki_df['occupation'] = mla_wiki_df['occupation'].replace({np.nan: None})
        mla_wiki_df['education'] = mla_wiki_df['education'].replace({np.nan: None})
        mla_wiki_df['years_active'] = mla_wiki_df['years_active'].replace({np.nan: None})
        mla_wiki_df['wiki_url'] = mla_wiki_df['wiki_url'].replace({np.nan: None})

        return mla_wiki_df.to_dict('records')

if __name__ == '__main__':
    time_before_running = time.time()
    print("Beginning scraper...")
    program_driver()
    time_after_running = time.time()
    time_elapsed = time_after_running - time_before_running
    print(f'Scraper ran in {time_elapsed} seconds')