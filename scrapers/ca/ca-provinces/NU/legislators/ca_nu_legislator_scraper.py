from operator import add, index
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
import pandas as pd
import numpy as np

BASE_URL = 'https://www.assembly.nu.ca'
MLA_URL = BASE_URL + '/members/mla'
WIKI_URL = 'https://en.wikipedia.org/wiki/Legislative_Assembly_of_Nunavut'
ELECTIONS_HISTORY_URL = 'https://www.elections.nu.ca/en/documents/election-results-and-financial-returns'
THREADS_FOR_POOL = 12

YEAR_TO_NTH_LEGISLATIVE_ASSEMBLY = {
                                    1999 : 1,
                                    2004 : 2,
                                    2008 : 3,
                                    2013 : 4,
                                    }

scraper_utils = CAProvTerrLegislatorScraperUtils('NU', 'ca_nu_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def program_driver():
    main_functions = MainFunctions()

    main_page_soup = main_functions.get_page_as_soup(MLA_URL)

    print('Getting data from MLA pages...')
    all_mla_links = MainSiteScraper().get_all_mla_links(main_page_soup)
    mla_data = main_functions.get_data_from_all_links(main_functions.get_mla_data, all_mla_links)
    print(mla_data)

class PreProgramFunctions:
    def set_legislative_office_address(self):
        page_soup = MainFunctions().get_page_as_soup(BASE_URL + '/contact-us-1')
        address_container = page_soup.find('div', {'class' : 'content-container'})

        index_where_address_ends = 2
        address_spans = address_container.findAll('span')[:index_where_address_ends]

        global LEGISLATIVE_ADDRESS
        LEGISLATIVE_ADDRESS = self.__extract_legislative_office_address(address_spans)

    def __extract_legislative_office_address(self, spans):
        address = ', '.join([span.text.strip() for span in spans])
        return address.replace(',,', ',')

    def update_term_and_legislature_dict(self, soup):
        most_recent_election_year = self.__get_most_recent_election_year(soup)
        self.__add_current_legislature_term_if_missing(most_recent_election_year)

        global CURRENT_LEGISLATURE_TERM
        CURRENT_LEGISLATURE_TERM = self.__get_current_legislature()

    def __get_most_recent_election_year(self, soup):
        sidebar = soup.find('ul', {'id' : 'document-tree'})
        all_election_years = sidebar.findAll('li')
        return self.__find_most_recent_year(all_election_years)

    def __find_most_recent_year(self, options):
        for option in options:
            if 'General Election' in option.text:
                return self.__extract_year(option.text)
        return -1

    def __extract_year(self, text):
        year = re.findall(r'[0-9]{4}', text)[0]
        return int(year)

    def __add_current_legislature_term_if_missing(self, year):
        if YEAR_TO_NTH_LEGISLATIVE_ASSEMBLY.get(year) == None:
            all_nth_assemblies = YEAR_TO_NTH_LEGISLATIVE_ASSEMBLY.values()
            most_recent_term = list(all_nth_assemblies)[-1]
            YEAR_TO_NTH_LEGISLATIVE_ASSEMBLY.setdefault(year, most_recent_term + 1)

    def __get_current_legislature(self):
        all_nth_assemblies = YEAR_TO_NTH_LEGISLATIVE_ASSEMBLY.values()
        return list(all_nth_assemblies)[-1]

class MainFunctions:        
    def get_page_as_soup(self, url):
        page_html = self.__get_site_as_html(url)
        return soup(page_html, 'html.parser')

    def __get_site_as_html(self, url):
        uClient = urlopen(url)
        page_html =uClient.read()
        uClient.close()
        scraper_utils.crawl_delay(crawl_delay)
        return page_html

    def get_data_from_all_links(self, function, all_links):
        data = []
        with Pool(THREADS_FOR_POOL) as pool:
            data = pool.map(func=function,
                            iterable=all_links)
        return data

    def get_mla_data(self, mla_url):
        return MLASiteScraper(mla_url).get_rows()

class MainSiteScraper:
    def get_all_mla_links(self, main_page_soup):
        main_container = main_page_soup.find('div', {'class' : 'content-container-inner'})
        divs_containing_a_tag = main_container.findAll('div', {'class' : 'views-field views-field-field-member-photo-fid'})
        return self.__get_individual_mla_links_from_div(divs_containing_a_tag)

    def __get_individual_mla_links_from_div(self, divs):
        mla_links = []
        for div in divs:
            mla_links.append(BASE_URL + div.find('a')['href'])
        return mla_links

class MLASiteScraper:
    def __init__(self, mla_url):
        self.row = scraper_utils.initialize_row()
        self.url = mla_url
        self.soup = MainFunctions().get_page_as_soup(self.url)
        self.main_container = self.soup.find('div', {'class' : 'content-container-inner'})
        self.__set_row_data()

    def get_rows(self):
        return self.row

    def __set_row_data(self):
        self.row.source_url = self.url
        self.__set_name_data()
        self.__set_riding_data()
        self.__set_role_data()
        self.__set_party_data()
        self.__set_contact_info()
        self.__set_most_recent_term_id()

    def __set_name_data(self):
        human_name = self.__get_full_human_name()
        self.row.name_full = human_name.full_name
        self.row.name_last = human_name.last
        self.row.name_first = human_name.first
        self.row.name_middle = human_name.middle
        self.row.name_suffix = human_name.suffix

    def __get_full_human_name(self):
        full_name = self.main_container.find('h1').text
        if ('onourable' in full_name):
            full_name = full_name.split('onourable')[1]
        return HumanName(full_name.strip())

    def __set_riding_data(self):
        container_for_electoral_district = self.main_container.find(
                'div', {'class' : 'field field-type-text field-field-member-mla'}
            )
        self.row.riding = self.__get_electoral_district(container_for_electoral_district)

    def __get_electoral_district(self, container):
        container_text = container.text
        riding = container_text.replace('Constituency:', '')
        return riding.strip()

    def __set_role_data(self):
        self.row.role = "Member of the Legislative Assembly"

    def __set_party_data(self):
        self.row.party = "Consensus Government"
        try:
            self.row.party_id = scraper_utils.get_party_id("Consensus Government")
        except Exception:
            self.row.party_id = 0

    def __set_contact_info(self):
        legislative_office_container = self.__get_legislative_office_container()
        constituency_office_container = self.__get_constituency_office_container()

        self.row.addresses = self.__get_addresses(constituency_office_container)
        self.row.email = self.__get_email(legislative_office_container)
        self.row.phone_numbers = self.__get_phone_numebers(legislative_office_container, constituency_office_container)

    def __get_legislative_office_container(self):
        return self.main_container.find('div', {'class' : 'field-field-member-legislative'})

    def __get_constituency_office_container(self):
        return self.main_container.find('div', {'class' : 'field-field-member-constituency'})

    def __get_addresses(self, constituency_office_container):
        member_office_address = {'location' : 'legislative office',
                                 'address' : LEGISLATIVE_ADDRESS}
        constituency_office_address = self.__extract_constituency_office_address(constituency_office_container)
        returnList = [member_office_address]
        returnList.extend(constituency_office_address)
        return returnList

    def __extract_constituency_office_address(self, container):
        constituency_containers = container.findAll('p')
        constituencies = []
        for container in constituency_containers:
            text = container.text
            text = text.split('Phone')[0]
            index_where_address_ends = 3
            address = ', '.join(text.split('\n')[:index_where_address_ends])
            address = address.replace(',,', ',')
            constituencies.append({'location' : 'constituency office',
                                   'address' : address})
        return constituencies

    def __get_email(self, container):
        '''
        Website is loaded by Javascript and changes the text
        Could use Selenium but this method will be faster.
        '''
        address_container = container.find('span', {'class' : 'spamspan'})
        email = address_container.text
        return self.__format_email_address(email)
    
    def __format_email_address(self, email):
        email = email.replace('[at]', '@')
        email = email.replace('[dot]', '.')
        return email.replace(' ', '')

    def __get_phone_numebers(self, legislative_container, constituency_container):
        legislative_numbers = self.__get__numbers_from_container(legislative_container)
        constituency_numbers = self.__get__numbers_from_container(constituency_container)
        index_of_phone_number = 0
        index_of_fax_number = 1

        legislative_phone = self.__get_number('legislative office phone', 
                                               legislative_numbers[index_of_phone_number])
        legislative_fax = self.__get_number('legislative office fax',
                                           legislative_numbers[index_of_fax_number])
        constituency_phone = self.__get_number('constituency office phone', 
                                               constituency_numbers[index_of_phone_number])
        constituency_fax = self.__get_number('constituency office fax', 
                                               constituency_numbers[index_of_fax_number])
        return [legislative_phone, legislative_fax, constituency_phone, constituency_fax]

    def __get__numbers_from_container(self, container):
        text = container.text
        return re.findall(r'\([0-9]{3}\) [0-9]{3}-[0-9]{4}', text)

    def __get_number(self, label, number):
        number = self.__clean_up_number(number)
        return {'office' : label,
                'number' : number}

    def __clean_up_number(self, number):
        number = number.replace('(', '').replace(')', '')
        number = number.replace('867 ', '867-')
        return number.strip()

    def __set_most_recent_term_id(self):
        self.row.most_recent_term_id = list(YEAR_TO_NTH_LEGISLATIVE_ASSEMBLY.keys())[-1]

PreProgramFunctions().set_legislative_office_address()
elections_page_soup = MainFunctions().get_page_as_soup(ELECTIONS_HISTORY_URL)
PreProgramFunctions().update_term_and_legislature_dict(elections_page_soup)

if __name__ == '__main__':
    program_driver()