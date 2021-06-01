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
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

BASE_URL = 'https://yukonassembly.ca'
MLA_URL = BASE_URL + '/mlas?field_party_affiliation_target_id=All&field_assembly_target_id=All&sort_by=field_last_name_value'
COMMITTEE_URL = BASE_URL + '/committees'
WIKI_URL = 'https://en.wikipedia.org/wiki/Yukon_Legislative_Assembly#Current_members'
NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR = {24 : 1978,
                                    25 : 1982,
                                    26 : 1985,
                                    27 : 1989,
                                    28 : 1992,
                                    29 : 1996,
                                    30 : 2000, 
                                    31 : 2002,
                                    32 : 2006,
                                    33 : 2011,
                                    34 : 2016,
                                    35 : 2021}
CURRENT_YEAR = datetime.datetime.now().year
THREADS_FOR_POOL = 12

scraper_utils = CAProvTerrLegislatorScraperUtils('YT', 'ca_yt_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

options = Options()
options.headless = True

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
columns_not_on_main_site = ['birthday', 'education', 'occupation', 'committees']

def program_driver():
    main_page_soup = get_page_as_soup(MLA_URL)
    scraper_for_main = ScraperForMainSite()

    print("Getting data from mla pages...")
    all_mla_links = scraper_for_main.get_all_mla_links(main_page_soup)
    mla_data = get_data_from_all_links(get_mla_data, all_mla_links)

    print("Getting data from wiki pages...")
    all_wiki_links = scrape_main_wiki_link(WIKI_URL)
    wiki_data = get_data_from_all_links(scraper_utils.scrape_wiki_bio, all_wiki_links)

    print("Getting committee data from committee pages...")
    main_committees_page_soup = get_page_as_soup(COMMITTEE_URL)
    scraper_for_committees = ScraperForCommitteesMainSite()
    all_committee_links = scraper_for_committees.get_all_commitee_links(main_committees_page_soup)
    unprocessed_committee_data = get_data_from_all_links(get_committee_data, all_committee_links)
    committee_data = organize_unproccessed_committee_data(unprocessed_committee_data)
  
    complete_data_set = configure_data(mla_data, wiki_data, committee_data)
    print('Writing data to database...')
    scraper_utils.write_data(complete_data_set)
    print("Complete")

def get_page_as_soup(url):
    page_html = get_site_as_html(url)
    return soup(page_html, 'html.parser')

def get_site_as_html(link_to_open):
    uClient = urlopen(link_to_open)
    page_html = uClient.read()
    uClient.close()
    scraper_utils.crawl_delay(crawl_delay)
    return page_html

def get_data_from_all_links(function, all_links):
    data = []
    with Pool(THREADS_FOR_POOL) as pool:
        data = pool.map(func=function,
                        iterable=all_links)
    return data

def get_mla_data(mla_url):
    scraper_for_mla = ScraperForMLAs(mla_url)
    return scraper_for_mla.get_rows()

def configure_data(mla_data, wiki_data, committee_data):
    mla_df = pd.DataFrame(mla_data)
    mla_df = mla_df.drop(columns = columns_not_on_main_site)
  
    wiki_df = pd.DataFrame(wiki_data)[
        ['birthday', 'education', 'name_first', 'name_last', 'occupation']
    ]

    mla_wiki_df = pd.merge(mla_df, wiki_df, 
                           how='left',
                           on=['name_first', 'name_last'])
    mla_wiki_df['birthday'] = mla_wiki_df['birthday'].replace({np.nan: None})
    mla_wiki_df['occupation'] = mla_wiki_df['occupation'].replace({np.nan: None})
    mla_wiki_df['education'] = mla_wiki_df['education'].replace({np.nan: None})

    committee_df = pd.DataFrame(committee_data)

    big_df = pd.merge(mla_wiki_df, committee_df,
                      how='left',
                      on=['name_first', 'name_last'])
    isna = big_df['committees'].isna()
    big_df.loc[isna, 'committees'] = pd.Series([[]] * isna.sum()).values

    return big_df.to_dict('records')

def scrape_main_wiki_link(wiki_link):
    wiki_urls = []
    page_html = get_site_as_html(wiki_link)
    # html parsing
    page_soup = soup(page_html, "html.parser")

    table = page_soup.find("table", {"class": "wikitable sortable"})
    table = table.findAll("tr")[1:]
    for tr in table:
        td = tr.findAll("td")[1]
        url = 'https://en.wikipedia.org' + (td.a["href"])

        wiki_urls.append(url)
    return wiki_urls

def get_committee_data(committee_url):
    scraper_for_committee = ScraperForCommittee(committee_url)
    scraper_utils.crawl_delay(crawl_delay)
    return scraper_for_committee.get_committee_data()

def organize_unproccessed_committee_data(raw_data):
    restructured_data = {}
    return_data = []
    for committee in raw_data:
        restructure_committee_data(committee, restructured_data)
    add_to_return_data(restructured_data, return_data)
    return return_data

def restructure_committee_data(committee, restructured_data):
    committee_name = list(committee.keys())[0]
    list_of_members = committee[committee_name]
    for member in list_of_members:
        add_member_committee_data(member, committee_name, restructured_data)

def add_member_committee_data(member, committee_name, restructured_data):
    member_full_name = list(member.keys())[0]
    role = member[member_full_name]
    if 'Hon.' in member_full_name:
        member_full_name = member_full_name.replace('Hon. ', '').strip()
    restructured_data.setdefault(member_full_name, [])
    list_of_member_committees = restructured_data[member_full_name]
    list_of_member_committees.append({'role' : role, 'committee' : committee_name})

def add_to_return_data(dict_of_members_and_committees, return_data):
    for member in dict_of_members_and_committees.keys():
        name_first = member.split(' ')[0]
        name_last = member.split(' ')[1]
        committees = dict_of_members_and_committees[member]
        return_data.append({'name_first' : name_first, 'name_last' : name_last, 'committees' : committees})

class ScraperForMainSite:  
    def get_all_mla_links(self, main_page_soup):
        mem_bios_urls = []
        list_of_url_spans = self.__get_list_of_member_url_span(main_page_soup)

        for span in list_of_url_spans:
            self.__extract_mla_url_from_span(span, mem_bios_urls)
        return mem_bios_urls

    def __get_list_of_member_url_span(self, main_page_soup):
        container_of_all_members = main_page_soup.find('div', {'class' : 'view-content'})
        return container_of_all_members.findAll('span')

    def __extract_mla_url_from_span(self, span, current_list_of_urls):
        try:
            link_to_member_bio = BASE_URL + span.a['href']
            self.__add_url_to_list(link_to_member_bio, current_list_of_urls)
        except Exception:
            pass
        return link_to_member_bio

    def __add_url_to_list(self, url, current_list_of_urls):
        if url not in current_list_of_urls:
            url = url.replace('\n', '')
            current_list_of_urls.append(url)

class ScraperForMLAs:
    def __init__(self, mla_url):
        self.row = scraper_utils.initialize_row()
        self.url = mla_url
        self.soup = get_page_as_soup(self.url)
        self.main_container = self.soup.find('div', {'class' : 'content'})
        self.__set_row_data()

    def get_rows(self):
        return self.row

    def __set_row_data(self):
        self.row.source_url = self.url
        self.__set_name_data()
        self.__set_role_data()
        self.__set_party_data()
        self.__set_riding_data()
        # Everyone has the same office address. That address is declared at the footer of page
        self.__set_address()
        self.__set_contact_info()
        self.__set_service_period()
        self.__set_most_recent_term_id()

    def __set_name_data(self):
        human_name = self.__get_full_human_name()
        self.row.name_full = human_name.full_name
        self.row.name_last = human_name.last
        self.row.name_first = human_name.first
        self.row.name_middle = human_name.middle
        self.row.name_suffix = human_name.suffix

    def __get_full_human_name(self):
        full_name = self.main_container.find('span').text
        full_name = full_name.replace('Hon. ', '').strip()
        return HumanName(full_name)
  
    def __set_role_data(self):
        role_container = self.main_container.find('div', {'class' : 'field--name-field-title'})
        self.row.role = "Member of the Legislative Assembly" if not role_container else role_container.text

    def __set_party_data(self):
        party_info_container = self.main_container.find('div', {'class' : 'field--name-field-party-affiliation'})
        party_info = party_info_container.text
        party_name = self.__edit_party_name(party_info)
        self.row.party = party_name
        self.__set_party_id(party_name)
  
    def __edit_party_name(self, party_name):
        if 'Liberal' in party_name:
            return 'Liberal'
        elif 'Democratic' in party_name:
            return 'New Democratic'
        else:
            return party_name
  
    def __set_party_id(self, party_name):
        try:
            self.row.party_id = scraper_utils.get_party_id(party_name)
        except:
            self.row.party_id = 0

    def __set_riding_data(self):
        riding = self.main_container.find('div', {'class' : 'field--name-field-constituency'}).text
        self.row.riding = riding

    def __set_address(self):
        full_address = self.__get_address()
        address_location = full_address[0]
        street_address = full_address[1] + " Y1A 1B2" #This is the address that's in the contact page. This may need to be changed if address is updated.
        street_address_info = {'location' :  address_location, 'address' : street_address}
        mailing_address = full_address[2]
        mailing_address_info = {'location' : "Mailing Address", 'address' : mailing_address}
        self.row.addresses = [street_address_info, mailing_address_info]
  
    def __get_address(self):
        '''
          This function returns part of a list (parts_of_address).
          This is because only the first three from the split address is relevant.
          If html structure changes, this may need to be fixed.
        '''
        page_footer = self.soup.find('footer')
        address_container = page_footer.find('div', {'class' : 'footer-row--right'})
        full_address = address_container.find('p').text
        full_address = full_address.replace('\xa0', '')
        parts_of_address = full_address.split('\n')
        return parts_of_address[:3]

    def __set_contact_info(self):
        contact_info = self.__get_contact_info()
        self.row.email = contact_info.a.text
        self.row.phone_numbers = self.__get_phone_numbers(contact_info)

    def __get_contact_info(self):
        profile_sidebar = self.soup.find('aside', {'class' : 'member-sidebar'})
        contact_info_container = profile_sidebar.find('article')
        return contact_info_container.findAll('p')[1]
  
    def __get_phone_numbers(self, contact_info):
        numbers = re.findall(r'[0-9]{3}-[0-9]{3}-[0-9]{4}', contact_info.text)
        return self.__categorize_numbers(numbers)

    def __categorize_numbers(self, numbers):
        categorized_numbers = []
        # phone_types is in this order because the website has the numbers ordered from phone then fax,
        # so effectively our numbers parm from above method will store numbers as such
        phone_types = ['phone', 'fax']
        for index, number in enumerate(numbers):
            info = {'office' : phone_types[index],
                    'number' : number}
            categorized_numbers.append(info)
        return categorized_numbers

    def __set_service_period(self):
        in_service_paragraph = self.__get_service_paragraph()
        service_periods = self.__get_service_periods(in_service_paragraph)
        self.row.years_active = service_periods
  
    def __get_service_paragraph(self):
        mla_summary_container = self.soup.find('div', {'class' : 'field--type-text-with-summary'})
        mla_summary_paragraphs = mla_summary_container.findAll('p')
        return self.__find_key_sentence_from_paragraph(mla_summary_paragraphs)

    def __find_key_sentence_from_paragraph(self, paragraphs):
        for paragraph in paragraphs:
            if 'elected to the Yukon Legislative Assembly' in paragraph.text:
                return paragraph.text

    def __get_service_periods(self, paragraph):
        service_periods_as_string = re.findall('\d\d[a-z]{2}', paragraph)
        service_periods_as_int = [int(period[0:2]) for period in service_periods_as_string]
        return self.__get_service_periods_as_years(service_periods_as_int)

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

    def __set_most_recent_term_id(self):
        self.row.most_recent_term_id = self.row.years_active[-1]

class ScraperForCommitteesMainSite:
    def get_all_commitee_links(self, soup):
        committee_urls = []
        list_of_url_li = self.__get_list_of_committee_url_li(soup)

        for li in list_of_url_li:
            self.__extract_committee_url_from_li(li, committee_urls)
        return committee_urls

    def __get_list_of_committee_url_li(self, soup):
        container_of_all_committee_links = soup.find('aside')
        container_of_all_li = container_of_all_committee_links.find('li', {'class' : 'expanded dropdown active active-trail'})
        return container_of_all_li.findAll('li')

    def __extract_committee_url_from_li(self, li, current_committee_list):
        if (self.__is_irrelevant_list(li)):
            return
        try:
            link_to_committee = BASE_URL + li.a['href']
            self.__add_url_to_list(link_to_committee, current_committee_list)
        except Exception:
            pass
        return link_to_committee
  
    def __is_irrelevant_list(self, li):
        return li.has_attr('class') and li['class'][0] == 'expanded'

    def __add_url_to_list(self, url, current_list_of_urls):
        if url not in current_list_of_urls:
            url = url.replace('\n', '')
            current_list_of_urls.append(url)

class ScraperForCommittee:
    def __init__(self, committee_url):
        self.url = committee_url
        self.driver = webdriver.Chrome('web_drivers/chrome_win_90.0.4430.24/chromedriver.exe', options=options)
        self.driver.switch_to.default_content()
        self.data = {}
        self.__collect_data()
  
    def get_committee_data(self):
        return self.data

    def __collect_data(self):
        self.__open_url()
        committee_name = self.__get_committee_name()
        names = self.__get_members_names(committee_name)
        self.data = {committee_name : names}
        self.driver.close()
        self.driver.quit()

    def __open_url(self):
        self.driver.get(self.url)
        self.driver.maximize_window()
        sleep(2)

    def __get_committee_name(self):
        return self.driver.find_element_by_class_name('page-header').text

    def __get_members_names(self, committee_name):
        if 'Special Committee on Civil Emergency Legislation' in committee_name:
            return self.__get_members_from_weird_special_committee()
        elif 'Select Committee on Bill 108, Legislative Renewal Act' in committee_name:
            return self.__get_members_from_special(1)
        elif 'Special' in committee_name or 'Select' in committee_name:
            return self.__get_members_from_special(0)
        else:
            return self.__get_members_from_normal()

    def __get_members_from_weird_special_committee(self):
        main_container = self.driver.find_element_by_class_name('content')
        member_containers = main_container.find_elements_by_class_name('paragraph--type--member')
        names = []
        for member in member_containers:
            try:
                name = member.find_element_by_tag_name('span').text
                names.append({name : 'member'})
            except Exception:
                pass
        return names

    def __get_members_from_special(self, ul_index):
        main_container = self.driver.find_element_by_class_name('col-md-8')
        members_container = main_container.find_elements_by_tag_name('ul')[ul_index]
        names_container = members_container.find_elements_by_tag_name('li')
        names = []
        for name in names_container:
            if '(' in name.text:
                member_name = name.text.split('(')[0]
                role = name.text.split('(')[1].split(')')[0]
            else:
                member_name = name.text
                role = 'member'
            names.append({member_name : role})
        return names

    def __get_members_from_normal(self):
        names_container = self.driver.find_element_by_xpath('/html/body/div[1]/div/div/section/div[2]/article/div/div[2]/div/div[2]/div[1]/div/div[2]')
        names = names_container.find_elements_by_tag_name('span')
        return [{name.text : 'member'} for name in names]

if __name__ == '__main__':
    program_driver()