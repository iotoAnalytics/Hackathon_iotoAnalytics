import sys
import os
from pathlib import Path
import re
import datetime
from time import sleep

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

BASE_URL = 'https://www.ntassembly.ca/'
MLA_URL = 'https://www.ntassembly.ca/members'
THREADS_FOR_POOL = 12

scraper_utils = CAProvTerrLegislatorScraperUtils('NT', 'ca_nt_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def program_driver():
    main_functions = Main_Functions()
    
    main_page_soup = main_functions.get_page_as_soup(MLA_URL)

    print("Getting data from MLA pages...")
    all_mla_links = Main_Site_Scraper().get_all_mla_links(main_page_soup)
    mla_data = main_functions.get_data_from_all_links(main_functions.get_mla_data, all_mla_links)


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

    def get_data_from_all_links(self, function, all_links):
        data = []
        with Pool(THREADS_FOR_POOL) as pool:
            data = pool.map(func=function,
                            iterable=all_links)
        return data

    def get_mla_data(self, mla_url):
        return MLA_Site_Scraper(mla_url).get_rows()

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
        return self.row

    def __set_row_data(self):
        self.row.source_url = self.url
        self.__set_name_data()
        self.__set_role_data()
        self.__set_party_data()
        self.__set_riding_data()
        self.__set_contact_info()

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
        potential_containers_for_electoral_district = self.main_container.findAll('u')
        riding = self.__find_electoral_district(potential_containers_for_electoral_district)
        if riding == '':
            potential_containers_for_electoral_district = self.main_container.findAll('strong')
            riding = self.__find_electoral_district(potential_containers_for_electoral_district)
        self.row.riding = riding
    
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
        address = self.__format_member_address(container)
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
        print(members_office_number)

    def __get_member_office_number(self, container):
        a_tag = container.a
        a_tag.decompose()
        contact_text = container.text
        contact_text = contact_text.replace('\xa0', '')
        if 'Phone number:' in contact_text:
            number = contact_text.split('Phone number:')[1].strip()
        else:
            number = contact_text.split('Ph')[1].strip()
        return self.__clean_up_member_office_number(number)
    
    def __clean_up_member_office_number(self, number):
        number = number.replace('(', '').replace(')', '')
        number = number.replace('ext.', ' ext. ').replace('  ', ' ')
        number = number.replace('867 ', '867-')
        length_of_phone_number_and_extension = 23
        return number[:length_of_phone_number_and_extension].strip()


# #content > div > div.field.field-name-body.field-type-text-with-summary.field-label-hidden > div > div > p:nth-child(7) > a > strong > u
# #content > div > div.field.field-name-body.field-type-text-with-summary.field-label-hidden > div > div > p:nth-child(7) > u > strong > a
# #content > div > div.field.field-name-body.field-type-text-with-summary.field-label-hidden > div > div > p:nth-child(6) > strong > u

if __name__ == '__main__':
    program_driver()
    
