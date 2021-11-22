import sys
import os
from pathlib import Path
import re
import datetime

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from scraper_utils import CAProvTerrLegislatorScraperUtils
from urllib.request import urlopen
from bs4 import BeautifulSoup as soup
from multiprocessing import Pool
from nameparser import HumanName
from unidecode import unidecode
import pandas as pd
import numpy as np

BASE_URL = 'https://www.ntassembly.ca'
MLA_URL = BASE_URL + '/members'
WIKI_URL = 'https://en.wikipedia.org/wiki/Legislative_Assembly_of_the_Northwest_Territories'
THREADS_FOR_POOL = 12

scraper_utils = CAProvTerrLegislatorScraperUtils('NT', 'ca_nt_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

CURRENT_YEAR = datetime.datetime.now().year

# These two will be updated in the program driver
CURRENT_LEGISLATURE_TERM = 0
NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR = {13 : 1995,
                                    14 : 1999,
                                    15 : 2003,
                                    16 : 2007,
                                    17 : 2011,
                                    18 : 2015,
                                    19 : 2019}

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
columns_not_on_main_site = ['birthday', 'education', 'occupation']

def program_driver():
    main_functions = Main_Functions()

    print("Getting data from MLA pages...")
    all_mla_links = Main_Site_Scraper().get_all_mla_links(main_page_soup)
    mla_data = main_functions.get_data_from_all_links(main_functions.get_mla_data, all_mla_links)
    while None in mla_data:
        mla_data.remove(None)

    print('Getting data from wiki pages...')
    all_wiki_links = main_functions.scrape_main_wiki_link(WIKI_URL)
    wiki_data = main_functions.get_data_from_all_links(scraper_utils.scrape_wiki_bio, all_wiki_links)

    complete_data_set = main_functions.configure_data(mla_data, wiki_data)
    print('Writing data to database...')
    scraper_utils.write_data(complete_data_set)
    print("Complete")

class Main_Functions:        
    def get_page_as_soup(self, url):
        page_html = self.__get_site_as_html(url)
        return soup(page_html, 'html.parser')

    def __get_site_as_html(self, url):
        uClient = urlopen(url)
        page_html =uClient.read()
        uClient.close()
        scraper_utils.crawl_delay(crawl_delay)
        return page_html

    def update_term_and_legislature_dict(self, soup):
        global CURRENT_LEGISLATURE_TERM
        CURRENT_LEGISLATURE_TERM = self.__get_current_legislature(soup)
        self.__add_current_legislature_term_if_missing(CURRENT_LEGISLATURE_TERM)

    def __get_current_legislature(self, soup):
        sidebar = soup.find('ul', {'class' : 'menu'})
        all_menu_item = sidebar.findAll('li')
        return self.__find_current_legislature(all_menu_item)

    def __find_current_legislature(self, options):
        for option in options:
            if self.__option_holds_legislature_term(option):
                return self.__extract_term_num(option)
        return -1
    
    def __option_holds_legislature_term(self, option):
        text = option.text
        return re.search('\d\d[a-z]{2}', text)

    def __extract_term_num(self, option):
        text = option.text
        service_period_as_string = re.findall('\d\d[a-z]{2}', text)[0]
        return int(service_period_as_string[0:2])

    def __add_current_legislature_term_if_missing(self, current_term):
        term_interval = 4
        if current_term not in NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR.keys():
            previous_year = NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR[current_term - 1]
            NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR[current_term] = previous_year + term_interval

    def get_data_from_all_links(self, function, all_links):
        data = []
        with Pool(THREADS_FOR_POOL) as pool:
            data = pool.map(func=function,
                            iterable=all_links)
        return data

    def get_mla_data(self, mla_url):
        return MLA_Site_Scraper(mla_url).get_rows()

    def scrape_main_wiki_link(self, wiki_link):
        wiki_urls = []
        page_soup = self.get_page_as_soup(wiki_link)

        table = page_soup.find("table", {"class": "wikitable sortable"})
        table = table.findAll("tr")[1:]
        for tr in table:
            td = tr.findAll("td")[0]
            url = 'https://en.wikipedia.org' + (td.a["href"])

            wiki_urls.append(url)
        return wiki_urls

    def configure_data(self, mla_data, wiki_data):
        mla_df = pd.DataFrame(mla_data)
        mla_df = mla_df.drop(columns = columns_not_on_main_site)
    
        wiki_df = pd.DataFrame(wiki_data)[
            ['birthday', 'education', 'wiki_url', 'occupation']
        ]

        mla_wiki_df = pd.merge(mla_df, wiki_df, 
                            how='left',
                            on=['wiki_url'])
        mla_wiki_df['birthday'] = mla_wiki_df['birthday'].replace({np.nan: None})
        mla_wiki_df['occupation'] = mla_wiki_df['occupation'].replace({np.nan: None})
        mla_wiki_df['education'] = mla_wiki_df['education'].replace({np.nan: None})

        return mla_wiki_df.to_dict('records')

class Main_Site_Scraper:
    def get_all_mla_links(self, main_page_soup):
        main_container = main_page_soup.find('table')
        spans_containing_a_tag = main_container.findAll('span')
        return self.__get_individual_mla_links_from_spans(spans_containing_a_tag)
        
    def __get_individual_mla_links_from_spans(self, spans):
        mla_links = []
        for span in spans:
            mla_links.append(BASE_URL + span.a['href'])
        return mla_links

class MLA_Site_Scraper:
    def __init__(self, mla_url):
        self.row = scraper_utils.initialize_row()
        self.url = mla_url
        self.soup = Main_Functions().get_page_as_soup(self.url)
        self.main_container = self.soup.find('div', {'id' : 'content'})
        self.__set_row_data()

    def get_rows(self):
        if self.row.name_full == 'VACANT':
            return None
        return self.row

    def __set_row_data(self):
        self.row.source_url = self.url
        self.__set_name_data()
        if self.row.name_full == 'VACANT':
            return
        self.__set_role_data()
        self.__set_party_data()
        self.__set_riding_data()
        self.__set_contact_info()
        self.__set_most_recent_term_id()
        self.__set_years_active()
        self.__set_committee_data()
        self.__set_gender()
        self.__set_wiki_url()

    def __set_name_data(self):
        human_name = self.__get_full_human_name()
        self.row.name_full = human_name.full_name
        self.row.name_last = human_name.last
        self.row.name_first = human_name.first
        self.row.name_middle = human_name.middle
        self.row.name_suffix = human_name.suffix

    def __get_full_human_name(self):
        full_name = self.main_container.find('h1').text
        return HumanName(full_name)

    def __set_role_data(self):
        self.row.role = "Member of the Legislative Assembly"

    def __set_party_data(self):
        self.row.party = "Consensus Government"
        try:
            self.row.party_id = scraper_utils.get_party_id("Consensus Government")
        except Exception:
            self.row.party_id = 0
    
    def __set_riding_data(self):
        potential_containers_for_electoral_district = self.main_container.findAll('p')
        riding = self.__find_electoral_district(potential_containers_for_electoral_district)
        if riding == '':
            potential_containers_for_electoral_district = self.main_container.findAll('strong')
            riding = self.__find_electoral_district(potential_containers_for_electoral_district)

        # Website is stupid and didn't format this for Jane yet
        if riding == '' and self.row.name_full == 'Jane Weyallon Armstrong':
            riding = "Monfwi"
        self.row.riding = riding.strip()
    
    def __find_electoral_district(self, list_of_containers):
        for container in list_of_containers:
            text = container.text
            if 'Electoral District' in text:
                text = text.replace(':', '')
                return text.replace('Electoral District', '')
        return ''

    def __set_contact_info(self):
        raw_members_office_contact_container = self.__find_members_office()
        raw_constituency_office_contact_container = self.__find_constituency_office()

        self.row.addresses = self.__set_address(raw_members_office_contact_container, raw_constituency_office_contact_container)
        self.row.email = self.__get_member_email(raw_members_office_contact_container)
        self.row.phone_numbers = self.__get_numbers(raw_members_office_contact_container, raw_constituency_office_contact_container)
    
    def __find_members_office(self):
        all_paragraphs_in_main_container = self.main_container.findAll('p')
        for paragraph in all_paragraphs_in_main_container:
            text = paragraph.text
            if 'P.O.' in text:
                return paragraph
    
    def __find_constituency_office(self):
        all_paragraphs_in_main_container = self.main_container.findAll('p')
        for index, paragraph in enumerate(all_paragraphs_in_main_container):
            text = paragraph.text
            if 'Constituency Office' in text:
                return all_paragraphs_in_main_container[index + 1]

    def __set_address(self, member_address_container, constituency_address_container):
        member_office_address = self.__get_member_office_address(member_address_container)
        constituency_office_address = self.__get_constituency_office_address(constituency_address_container)
        return [member_office_address, constituency_office_address] if constituency_office_address else [member_office_address]

    def __get_member_office_address(self, container):
        '''
        It seems that all member's have the same member office address. 
        This function exists in case that it might change. 
        '''
        try:
            address = self.__format_member_address(container)
        except Exception as e:
            print(self.row.name_full)
        return {'location' : 'member\'s office',
                'address' : address}
        
    def __format_member_address(self, container):
        container_text = container.text
        address = container_text.split('Phone')[0]
        address = address.replace('\n', '').replace('\xa0', '')
        address = address.replace('Yellowknife', ' Yellowknife,').replace(',,', ',').replace('  ', ' ')
        return address

    def __get_constituency_office_address(self, container):
        try:
            container_text = container.text
        except Exception:
            return
        address = container_text.split('Ph')[0]
        if address == '':
            return
        address = self.__format_constituency_address(address)
        return {'location' : 'constituency office',
                'address' : address}

    def __format_constituency_address(self, text):
        address = text.replace('\n', ' ').replace('\xa0', '')
        address = address.replace('  ', ' ')
        return address.strip()

    def __get_member_email(self, container):
        email = container.find('a')['href']
        email = email.replace('mailto:', '')
        return email.capitalize()

    def __get_numbers(self, member_address_container, constituency_address_container):
        members_office_number = self.__get_member_office_number(member_address_container)
        m_office_number_to_add = {'office' : 'member\'s office',
                                  'number' : members_office_number}

        mobile_number = self.__get_mobile_number(member_address_container)
        mobile_number_to_add = {'office' : 'mobile',
                                'number' : mobile_number} if mobile_number else None
        
        constituency_office_number = self.__get_constituency_office_number(constituency_address_container)
        c_office_number_to_add = {'office' : 'constituency office',
                                      'number' : constituency_office_number} if constituency_office_number else None
        
        if not mobile_number_to_add and not c_office_number_to_add:
            return [m_office_number_to_add]
        if not mobile_number_to_add:
            return [m_office_number_to_add, c_office_number_to_add]
        if not c_office_number_to_add:
            return [m_office_number_to_add, mobile_number_to_add]

    def __get_member_office_number(self, container):
        a_tag = container.a
        a_tag.decompose()
        contact_text = container.text
        contact_text = contact_text.replace('\xa0', '')
        if 'Phone number:' in contact_text:
            number = contact_text.split('Phone number:')[1].strip()
        else:
            number = contact_text.split('Ph')[1].strip()
        length_of_phone_number_and_extension = 23
        return self.__clean_up_number(number, length_of_phone_number_and_extension)

    def __get_mobile_number(self, container):
        contact_text = container.text
        try:
            contact_text = contact_text.split('Mobile: ')[1].strip()
            return self.__clean_up_number(contact_text, 13)
        except Exception:
            pass

    def __get_constituency_office_number(self, container):
        try:
            contact_text = container.text
        except Exception:
            return
        if 'Phone number:' in contact_text:
            number = contact_text.split('Phone number:')[1].strip()
        elif 'Ph:' in contact_text:
            number = contact_text.split('Ph:')[1].strip()
        else:
            return
        length_of_phone_number_and_extension = 23
        return self.__clean_up_number(number, length_of_phone_number_and_extension)
    
    def __clean_up_number(self, number, number_length):
        number = number.replace('(', '').replace(')', '')
        number = number.replace('ext.', ' ext. ').replace('  ', ' ')
        number = number.replace('ext', 'ext.').replace('..', '.')
        number = number.replace('867 ', '867-')
        return number[:number_length].strip()

    def __set_most_recent_term_id(self):
        self.row.most_recent_term_id = str(NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR[CURRENT_LEGISLATURE_TERM])

    def __set_years_active(self):
        biography_paragraphs = self.__get_biography()
        terms_worked = self.__find_terms_worked(biography_paragraphs)
        self.row.years_active = self.__get_service_periods_as_years(terms_worked)

    def __get_biography(self):
        all_paragraphs_in_main_container = self.main_container.findAll('p')
        biography_paragraphs = None

        for index, paragraph in enumerate(all_paragraphs_in_main_container):
            text = paragraph.text
            if 'Biography' in text:
                biography_paragraphs = all_paragraphs_in_main_container[index + 1:]
        
        if not biography_paragraphs:
            biography_paragraphs = all_paragraphs_in_main_container

        for index, paragraph in enumerate(biography_paragraphs):
            text = paragraph.text
            if 'Oath of Office' in text:
                return biography_paragraphs[:index]

        return biography_paragraphs

    def __find_terms_worked(self, paragraphs):
        terms_worked = set({})
        for paragraph in paragraphs:
            self.__add_term_if_exists(paragraph.text, terms_worked)
        return terms_worked
    
    def __add_term_if_exists(self, paragraph, set_to_add_to):
        service_periods_as_string = re.findall('\d\d[a-z]{2}', paragraph)
        for period in service_periods_as_string:
            set_to_add_to.add(int(period[0:2]))

    def __get_service_periods_as_years(self, service_periods_as_int):
        service_periods_as_years = []
        for period in service_periods_as_int:
            self.__add_periods_as_years(period, service_periods_as_years)
        return service_periods_as_years
        
    def __add_periods_as_years(self, period, return_list):
        last_period = list(NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR)[-1]
        current_term_year = NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR.get(period)
        if period != last_period:
            next_term_year = NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR.get(period + 1)
            for i in range(current_term_year, next_term_year):
                return_list.append(i)
        elif CURRENT_YEAR > period:
            for i in range(current_term_year, CURRENT_YEAR + 1):
                return_list.append(i)

    def __set_committee_data(self):
        committees_container = self.__get_committee_paragraph()
        try:
            list_of_committees = self.__get_committees_list(committees_container)
            self.row.committees = [self.__extract_committee_data(committee) for committee in list_of_committees]
        except Exception:
            self.row.committees = []
            return

    def __get_committee_paragraph(self):
        all_paragraphs_in_main_container = self.main_container.findAll('p')
        for index, paragraph in enumerate(all_paragraphs_in_main_container):
            text = paragraph.text
            if 'Committees' in text:
                return all_paragraphs_in_main_container[index + 1]
    
    def __get_committees_list(self, container):
        text = container.text
        return text.split('\n')

    def __extract_committee_data(self, committee):
        committee_name = committee.split(' - ')[0]
        role = committee.split(' - ')[1]
        return {'role' : role,
                'committee' : committee_name}

    def __set_gender(self):
        self.row.gender = scraper_utils.get_legislator_gender(self.row.name_first, self.row.name_last, self.main_container.text)

    def __set_wiki_url(self):
        page_soup = Main_Functions().get_page_as_soup(WIKI_URL)

        table = page_soup.find("table", {"class": "wikitable sortable"})
        table = table.findAll("tr")[1:]

        for tr in table:
            district = tr.findAll("td")[1].text
            name_td = tr.findAll("td")[0]
            name = name_td.text
            if unidecode(self.row.riding.lower()) == unidecode(district.strip().lower()) and unidecode(self.row.name_last.lower()) in unidecode(name.strip().lower()):
                self.row.wiki_url = 'https://en.wikipedia.org' + name_td.a['href']
                return
        print(f'wiki_link not found for: {self.row.name_full}')


# updates global variables for nth term and current legislature year
main_page_soup = Main_Functions().get_page_as_soup(MLA_URL)
Main_Functions().update_term_and_legislature_dict(main_page_soup)

try:
    if __name__ == '__main__':
        program_driver()
except Exception as e:
    print(e)
    sys.exit(1)