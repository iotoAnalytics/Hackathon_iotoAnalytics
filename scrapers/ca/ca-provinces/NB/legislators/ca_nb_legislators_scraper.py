import os
import sys
import traceback

from pathlib import Path

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

import numpy as np
import pandas as pd
import re
import time

from bs4 import BeautifulSoup as soup
from multiprocessing import Pool
from nameparser import HumanName as hn
from scraper_utils import CAProvTerrLegislatorScraperUtils
from urllib.request import urlopen
from unidecode import unidecode

BASE_GOV_WEBSITE = 'https://www.gnb.ca'
GOV_SITE_WITH_LINK_TO_MLAS = BASE_GOV_WEBSITE + '/legis/index-e.asp'

MLA_CONTACT_BASE_URL = 'https://www2.gnb.ca/content/gnb/en/contacts'
MLA_REPORT_URL = MLA_CONTACT_BASE_URL + '/MLAReport.html'

COMMITTEES_BASE_URL = 'https://www1.gnb.ca/legis/committees/'
COMMITTEES_MAIN_URL = COMMITTEES_BASE_URL + 'comm-index-e.asp'

WIKI_BASE_URL = 'https://en.wikipedia.org'
WIKI_URL = WIKI_BASE_URL + '/wiki/Legislative_Assembly_of_New_Brunswick'

THREADS_FOR_POOL = 12

COMMITTEES = []

scraper_utils = CAProvTerrLegislatorScraperUtils('NB', 'ca_nb_legislators')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

COLUMNS_NOT_ON_MAIN_SITE = ["years_active", "birthday", "occupation", "education"]

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
        self.row.wiki_url = self.__get_wiki_url()
        self.row.committees = self.__get_committee_data()

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

    def __get_wiki_url(self):
        page_soup = Utils().get_page_as_soup(WIKI_URL)
        table = page_soup.find("table", {"class": "wikitable sortable"}).find('tbody')
        trs = table.findAll("tr")[1:]
        for tr in trs:
            name_td = tr.findAll("td")[1]
            name = name_td.text
            district = tr.findAll("td")[3].text
            
            if "Vacant" in name:
                continue

            if unidecode(self.row.riding.lower()) == unidecode(district.strip().lower()) and unidecode(self.row.name_last.lower()) in unidecode(name.strip().lower()):
                return WIKI_BASE_URL + name_td.a["href"]

    def __get_committee_data(self):
        committees = []
        for committee in COMMITTEES:
            membership = committee.get_committee_member_bio_urls()
            try:
                role = membership[self.bio_url]
                committees.append({
                    "role": role,
                    "committee": committee.get_committee_name()
                })
            except:
                pass
        return committees

class Committee:
    def __init__(self, url, name):
        self.url = url
        self.committee_name = name

        self.page_soup = Utils().get_page_as_soup(url)
        self.members_bio_urls = self.__find_bio_urls()

    def get_committee_name(self):
        return self.committee_name

    def get_committee_member_bio_urls(self):
        return self.members_bio_urls

    def __find_bio_urls(self):
        bio_urls = {}
        div = self.page_soup.find('div', {'class':'grid_6 alpha'})
        member_divs = div.find_all(self.__is_div_and_has_a_tag)
        for div in member_divs:
            if len(div.find_all("br")) == 2:
                role = div.find_all("br")[-1].nextSibling.strip()
            else:
                role = "member"
            bio_urls[div.a["href"]] = role
        return bio_urls

    def __is_div_and_has_a_tag(self, tag):
        return tag.a is not None and tag.name == 'div' and tag.has_attr('class')

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

    def get_committees_urls_and_names(self):
        page_soup = self.get_page_as_soup(COMMITTEES_MAIN_URL)
        div = page_soup.find("div", {'id':'committees-menu'})
        relevant_uls = div.find_all('ul')[:-1]
        lis = []
        for ul in relevant_uls:
            lis.extend(ul.find_all('li'))

        urls = []
        for li in lis:
            urls.append({
                "url": COMMITTEES_BASE_URL + li.a["href"],
                "committee_name": li.text.strip()
            })
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
            "birthday", "years_active", "wiki_url", "occupation", "education"
        ]]

        mla_wiki_df = pd.merge(mla_df, wiki_df, 
                            how='left',
                            on=['wiki_url'])
        mla_wiki_df['birthday'] = mla_wiki_df['birthday'].replace({np.nan: None})
        mla_wiki_df['occupation'] = mla_wiki_df['occupation'].replace({np.nan: None})
        mla_wiki_df['education'] = mla_wiki_df['education'].replace({np.nan: None})
        mla_wiki_df['years_active'] = mla_wiki_df['years_active'].replace({np.nan: None})

        return mla_wiki_df.to_dict('records')

committees_urls_and_names = Utils().get_committees_urls_and_names()
for url_and_name in committees_urls_and_names:
    COMMITTEES.append(Committee(url_and_name.get('url'), url_and_name.get('committee_name')))

try:
    if __name__ == '__main__':
        time_before_running = time.time()
        print("Beginning scraper...")
        program_driver()
        time_after_running = time.time()
        time_elapsed = time_after_running - time_before_running
        print(f'Scraper ran in {time_elapsed} seconds')
except Exception as e:
    traceback.print_exc()
    sys.exit(1)