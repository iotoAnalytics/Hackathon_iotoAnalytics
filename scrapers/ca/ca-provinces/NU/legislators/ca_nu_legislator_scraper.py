import datetime
import os
import re
import sys
import traceback

from pathlib import Path

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import pandas as pd
import numpy as np

from bs4 import BeautifulSoup as soup
from bs4 import NavigableString
from multiprocessing import Pool
from nameparser import HumanName
from scraper_utils import CAProvTerrLegislatorScraperUtils
from urllib.request import urlopen
from unidecode import unidecode

BASE_URL = 'https://www.assembly.nu.ca'
MLA_URL = BASE_URL + '/members/mla'
WIKI_URL = 'https://en.wikipedia.org/wiki/Legislative_Assembly_of_Nunavut'
COMMITTEE_URL = BASE_URL + '/standing-and-special-committees'

# NOTE The website seems to change the layout sometimes. Might need to check.
ELECTIONS_HISTORY_URL = 'https://www.elections.nu.ca/en/documents'
THREADS_FOR_POOL = 12

CURRENT_YEAR = datetime.datetime.now().year
NTH_TO_YEAR_LEGISLATIVE_ASSEMBLY = {
                                    1 : 1999,
                                    2 : 2004,
                                    3 : 2008,
                                    4 : 2013,
                                    5 : 2017,
                                    6 : 2021,
                                    }

scraper_utils = CAProvTerrLegislatorScraperUtils('NU', 'ca_nu_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
COLUMNS_NOT_ON_MAIN_SITE = ['birthday', 'education', 'occupation', 'committees']

def program_driver():
    main_functions = MainFunctions()

    main_page_soup = main_functions.get_page_as_soup(MLA_URL)

    print('Getting data from MLA pages...')
    all_mla_links = MainSiteScraper().get_all_mla_links(main_page_soup)
    mla_data = main_functions.get_data_from_all_links(main_functions.get_mla_data, all_mla_links)

    print('Getting data from wiki pages...')
    all_wiki_links = main_functions.scrape_main_wiki_link(WIKI_URL)
    wiki_data = main_functions.get_data_from_all_links(scraper_utils.scrape_wiki_bio, all_wiki_links)

    print("Getting committee data from committee pages...")
    main_committees_page_soup = main_functions.get_page_as_soup(COMMITTEE_URL)
    all_committee_links = CommitteeMainSiteScraper().get_all_committee_links(main_committees_page_soup)
    index_where_committees_end = 4
    unprocessed_committee_data = main_functions.get_data_from_all_links(main_functions.get_committee_data, 
                                                                        all_committee_links[:index_where_committees_end + 1])
    committee_data = main_functions.organize_unproccessed_committee_data(unprocessed_committee_data)

    complete_data_set = main_functions.configure_data(mla_data, wiki_data, committee_data)
    print('Writing data to database...')
    scraper_utils.write_data(complete_data_set)
    print("Complete")

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
        content = soup.find('ul', {'id' : 'document-tree'})

        election_results_and_financial_returns = content.find(id='tid-37')
        election_results = election_results_and_financial_returns.findAll('a')
        return self.__find_most_recent_year(election_results)

    def __find_most_recent_year(self, options):
        for option in options:
            if 'General Election' in option.text:
                return self.__extract_year(option.text)
        return -1

    def __extract_year(self, text):
        year = re.findall(r'[0-9]{4}', text)[0]
        return int(year)

    def __add_current_legislature_term_if_missing(self, year):
        if year not in NTH_TO_YEAR_LEGISLATIVE_ASSEMBLY.values():
            all_nth_assemblies = NTH_TO_YEAR_LEGISLATIVE_ASSEMBLY.keys()
            most_recent_term = list(all_nth_assemblies)[-1]
            NTH_TO_YEAR_LEGISLATIVE_ASSEMBLY.setdefault(most_recent_term + 1, year)

    def __get_current_legislature(self):
        all_nth_assemblies = NTH_TO_YEAR_LEGISLATIVE_ASSEMBLY.keys()
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

    def scrape_main_wiki_link(self, wiki_link):
        wiki_urls = []
        page_soup = self.get_page_as_soup(wiki_link)

        table = page_soup.find("table", {"class": "wikitable sortable"})
        table = table.findAll("tr")[1:]
        for tr in table:
            td = tr.findAll("td")[1]
            url = 'https://en.wikipedia.org' + (td.a["href"])

            wiki_urls.append(url)
        return wiki_urls

    def get_committee_data(self, committee_url):
        return CommitteeSiteScraper(committee_url).get_committee_data()

    def organize_unproccessed_committee_data(self, raw_data):
        restructured_data = {}
        return_data = []
        for committee in raw_data:
            self.__restructure_committee_data(committee, restructured_data)
        self.__add_to_return_data(restructured_data, return_data)
        return return_data

    def __restructure_committee_data(self, committee, restructured_data):
        committee_name = list(committee.keys())[0]
        list_of_members = committee[committee_name]
        for member in list_of_members:
            self.__add_member_committee_data(member, committee_name, restructured_data)

    def __add_member_committee_data(self, member, committee_name, restructured_data):
        member_full_name = list(member.keys())[0]
        role = member[member_full_name]
        restructured_data.setdefault(member_full_name, [])
        list_of_member_committees = restructured_data[member_full_name]
        list_of_member_committees.append({'role' : role, 'committee' : committee_name})

    def __add_to_return_data(self, dict_of_members_and_committees, return_data):
        for member in dict_of_members_and_committees.keys():
            name_first = member.split(' ')[0]
            name_last = member.split(' ')[1]
            committees = dict_of_members_and_committees[member]
            return_data.append({'name_first' : name_first, 'name_last' : name_last, 'committees' : committees})

    def configure_data(self, mla_data, wiki_data, committee_data):
        mla_df = pd.DataFrame(mla_data)
        mla_df = mla_df.drop(columns = COLUMNS_NOT_ON_MAIN_SITE)
    
        wiki_df = pd.DataFrame(wiki_data)[
            ['birthday', 'education', 'wiki_url', 'occupation']
        ]

        mla_wiki_df = pd.merge(mla_df, wiki_df, 
                               how='left',
                               on=['wiki_url'])
        mla_wiki_df['birthday'] = mla_wiki_df['birthday'].replace({np.nan: None})
        mla_wiki_df['occupation'] = mla_wiki_df['occupation'].replace({np.nan: None})
        mla_wiki_df['education'] = mla_wiki_df['education'].replace({np.nan: None})

        committee_df = pd.DataFrame(committee_data)

        big_df = pd.merge(mla_wiki_df, committee_df,
                          how='left',
                          on=['name_first', 'name_last'])
        isna = big_df['committees'].isna()
        big_df.loc[isna, 'committees'] = pd.Series([[]] * isna.sum()).values
        isna = big_df['occupation'].isna()
        big_df.loc[isna, 'occupation'] = pd.Series([[]] * isna.sum()).values
        isna = big_df['education'].isna()
        big_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values

        return big_df.to_dict('records')

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
        self.__set_years_active()
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
        riding = riding.replace(' - ', '-')
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
        self.row.email = self.__get_email(legislative_office_container, constituency_office_container)
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

    def __get_email(self, container1, container2):
        '''
        Website is loaded by Javascript and changes the text
        Could use Selenium but this method will be faster.
        '''
        address_container = container1.find('a', {'class' : 'spamspan'})
        try:
            email = address_container.text
        except:
            print(f"No legislative email currently for member {self.row.name_full}")
        
        try:
            address_container = container2.find('a', {'class' : 'spamspan'})
            email = address_container.text
        except:
            print(f"No legislative email currently for member {self.row.name_full}")
            email = ''
            
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
        return_list = []
        try:
            legislative_phone = self.__get_number('legislative office phone', 
                                                legislative_numbers[index_of_phone_number])
            legislative_fax = self.__get_number('legislative office fax',
                                            legislative_numbers[index_of_fax_number])
            return_list.extend([legislative_phone, legislative_fax])
        except:
            print(f"No legislative numbers currently for member {self.row.name_full}")
            
        try:
            constituency_phone = self.__get_number('constituency office phone', 
                                                constituency_numbers[index_of_phone_number])
            constituency_fax = self.__get_number('constituency office fax', 
                                                constituency_numbers[index_of_fax_number])
            return_list.extend([constituency_phone, constituency_fax])
        except:
            print(f"No constituency numbers currently for member {self.row.name_full}")
            
        return return_list

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
        self.row.most_recent_term_id = str(NTH_TO_YEAR_LEGISLATIVE_ASSEMBLY[CURRENT_LEGISLATURE_TERM])

    def __set_years_active(self):
        member_information = self.main_container.find('div', {'class' : 'content clear-block'})
        terms_worked = set({})
        for child in member_information.children:
            if child.name == 'p':
                terms_worked.update(self.__find_years_active(child.text)) 
        self.row.years_active = self.__get_service_periods_as_years(terms_worked)

    def __find_years_active(self, string):
        return_list = []
        terms = self.__extract_terms_worked(string, 'Legislative Assembly')
        terms.extend(self.__extract_terms_worked(string, 'Legislative Assemblies'))
        return_list.extend(terms)
        return_list.append('5th')
        return set(return_list)

    def __extract_terms_worked(self, string, split_word):
        split_paragraphs = string.split(split_word)
        relevant_texts = split_paragraphs[:-1] # The last paragraph contains the unnecessary split part
        if 'Assembly' in split_word:
            return [text.split()[-1] for text in relevant_texts]
        elif 'Assemblies' in split_word:
            return_list = []
            for text in relevant_texts:
                return_list.extend(text.split()[-4:])
            return return_list

    def __get_service_periods_as_years(self, terms_worked):
        service_periods_as_years = []
        for term in terms_worked:
            self.__add_periods_as_years(term, service_periods_as_years)
        service_periods_as_years.sort()
        return service_periods_as_years

    def __add_periods_as_years(self, period, return_list):
        if not self.__is_period(period):
            return
        period_as_int = self.__convert_period_to_int(period)
        last_period = list(NTH_TO_YEAR_LEGISLATIVE_ASSEMBLY)[-1]
        current_term_year = NTH_TO_YEAR_LEGISLATIVE_ASSEMBLY.get(period_as_int)
        if period_as_int != last_period:
            next_term_year = NTH_TO_YEAR_LEGISLATIVE_ASSEMBLY.get(period_as_int + 1)
            for i in range(current_term_year, next_term_year):
                return_list.append(i)
        elif CURRENT_YEAR > period_as_int:
            for i in range(current_term_year, CURRENT_YEAR + 1):
                return_list.append(i)

    def __is_period(self, period):
        if 'the' in period or 'and' in period:
            return False
        return True

    def __convert_period_to_int(self, period):
        period = period.lower()
        if period == '1st' or period == 'first':
            return 1
        elif period == '2nd' or period == 'second':
            return 2
        elif period == '3rd' or period == 'third':
            return 3
        elif period == '4th' or period == 'fourth':
            return 4
        elif period == '5th' or period == 'fifth':
            return 5
        elif period == '6th' or period == 'sixth':
            return 6
        elif period == '7th' or period == 'seventh':
            return 7
        elif period == '8th' or period == 'eigth':
            return 8
        elif period == '9th' or period == 'ninth':
            return 9
        elif period == '10th' or period == 'tenth':
            return 10
        elif period == '11th' or period == 'eleventh':
            return 11
        else:
            return 0

    def __set_gender(self):
        self.row.gender = scraper_utils.get_legislator_gender(self.row.name_first, self.row.name_last, self.main_container.text)

    def __set_wiki_url(self):
        page_soup = MainFunctions().get_page_as_soup(WIKI_URL)

        table = page_soup.find("table", {"class": "wikitable sortable"})
        table = table.findAll("tr")[1:]

        for tr in table:
            name_td = tr.findAll("td")[1]
            name = name_td.text

            district = tr.findAll("td")[0].text
            if unidecode(self.row.riding.lower()) == unidecode(district.strip().lower()) and unidecode(self.row.name_last.lower()) in unidecode(name.strip().lower()):
                self.row.wiki_url = 'https://en.wikipedia.org' + name_td.a['href']
                return
        print(f'wiki_link not found for: {name}')
            
            
class CommitteeMainSiteScraper:
    def get_all_committee_links(self, soup):
        content = soup.find('div', {'class' : 'content-container-inner'})
        return self.__get_all_committee_links(content)

    def __get_all_committee_links(self, container):
        committee_lists = container.findAll('li')
        return self.__extract_link(committee_lists)

    def __extract_link(self, list_of_committtees):
        return [BASE_URL + list_element.a['href'] for list_element in list_of_committtees]

class CommitteeSiteScraper:
    def __init__(self, committee_url):
        self.url = committee_url
        self.soup = MainFunctions().get_page_as_soup(self.url)
        self.main_container = self.soup.find('div', {'class' : 'content-container-inner'})
        self.data = {}
        self.__collect_data()

    def get_committee_data(self):
        return self.data

    def __collect_data(self):
        committee_name = self.__get_committee_name()
        membership = self.__get_committee_membership()
        self.data = {committee_name : membership}
        
    def __get_committee_name(self):
        return self.main_container.find('h1').text.title()

    def __get_committee_membership(self):
        membership_information = self.main_container.findAll('p', {'style' : 'text-align: center;'})
        membership = []
        for role_member_container in membership_information:
            self.__extract_members(role_member_container, membership)
        return membership

    def __extract_members(self, container, return_list):
        for child in container.children:
            try:
                role = container.find('span').text
            except:
                role = 'Unknown'
            child_text = self.__get_text_from_navigable_string(child)

            if not child_text:
                pass
            elif role != 'Staff':
                data = self.__get_name_and_role(role, child_text)
                return_list.append(data)
            elif role == 'Staff':
                self.__get_name_and_role_from_staff(role, child_text, return_list)

    def __get_text_from_navigable_string(self, child):
        if type(child) == NavigableString:
            child = str(child.string)
            return self.__clean_up_text(child)
        else:
            return None

    def __clean_up_text(self, text):
        return text.replace('\n', '').replace('\xa0', '')

    def __get_name_and_role(self, role, text):
        if role == 'Members':
            role = 'Member'
        return {text : role} 

    def __get_name_and_role_from_staff(self, role, text, return_list):
        role = text.split(': ')[0]
        name = text.split(': ')[1]
        if 'and' in name:
            names = name.split(' and ')
            for actual_name in names:
                return_list.append({actual_name : role})
        else:
            return_list.append({name : role})

PreProgramFunctions().set_legislative_office_address()
elections_page_soup = MainFunctions().get_page_as_soup(ELECTIONS_HISTORY_URL)
PreProgramFunctions().update_term_and_legislature_dict(elections_page_soup)
# print(CURRENT_LEGISLATURE_TERM)
# print(NTH_TO_YEAR_LEGISLATIVE_ASSEMBLY)

try:
    if __name__ == '__main__':
        program_driver()
except Exception as e:
    traceback.print_exc()
    sys.exit(1)