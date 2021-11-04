from logging import error
import sys
import os
from pathlib import Path
import re
import datetime
from time import sleep

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from scraper_utils import USStateLegislatorScraperUtils
from urllib.request import urlopen
from bs4 import BeautifulSoup as soup
from multiprocessing import Pool
from nameparser import HumanName
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import ssl
import unidecode
ssl._create_default_https_context = ssl._create_unverified_context

WASHINGTON_STATE_LEGISLATURE_BASE_URL = 'https://leg.wa.gov/'
REPRESENTATIVE_PAGE_URL = WASHINGTON_STATE_LEGISLATURE_BASE_URL + 'house/representatives/Pages/default.aspx'
SENATOR_PAGE_URL = WASHINGTON_STATE_LEGISLATURE_BASE_URL + 'Senate/Senators/Pages/default.aspx'
ALL_MEMBER_EMAIL_LIST_URL = 'https://app.leg.wa.gov/MemberEmail/Default.aspx?Chamber=H'
ALL_MEMBER_COUNTY_LIST_URL = 'https://app.leg.wa.gov/Rosters/MembersByDistrictAndCounties'

WIKI_BASE_URL = 'https://en.wikipedia.org'
WIKI_URL = WIKI_BASE_URL+ '/wiki/Washington_State_Legislature'
BALLOTPEDIA_SEN = 'https://ballotpedia.org/Washington_State_Senate'
BALLOTPEDIA_REP = 'https://ballotpedia.org/Washington_House_of_Representatives'

THREADS_FOR_POOL = 12
CURRENT_YEAR = datetime.datetime.now().year

scraper_utils = USStateLegislatorScraperUtils('WA', 'us_wa_legislators')

# Maybe separate into different classes and use init to initalize these delays
state_legislature_crawl_delay = scraper_utils.get_crawl_delay(WASHINGTON_STATE_LEGISLATURE_BASE_URL)
wiki_crawl_delay = scraper_utils.get_crawl_delay(WIKI_BASE_URL)

options = Options()
options.headless = True

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
columns_not_on_main_site = ['birthday', 'education', 'occupation']

def program_driver():
    print("Gathering data...")
    mla_data = MainScraper("Representative").get_data()
    mla_data.extend(MainScraper("Senator").get_data())

    all_wiki_links = WikiScraper().scrape_main_wiki_link(BALLOTPEDIA_REP) + WikiScraper().scrape_main_wiki_link(BALLOTPEDIA_SEN)
    wiki_data = WikiScraper().get_data_from_all_links(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)

    print("Configuring data...")
    complete_data_set = configure_data(mla_data, wiki_data)
    
    print('Writing data to database...')
    scraper_utils.write_data(complete_data_set)
    print("Complete")
    
def configure_data(mla_data, wiki_data):
    mla_df = pd.DataFrame(mla_data)
    mla_df = mla_df.drop(columns = columns_not_on_main_site)
  
    wiki_df = pd.DataFrame(wiki_data)[
        ['birthday', 'education', 'name_first', 'name_last', 'occupation', 'wiki_url']
    ]

    big_df = pd.merge(mla_df, wiki_df, 
                           how='left',
                           on=['wiki_url'])
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df['occupation'] = big_df['occupation'].replace({np.nan: None})
    big_df['education'] = big_df['education'].replace({np.nan: None})

    isna = big_df['occupation'].isna()
    big_df.loc[isna, 'occupation'] = pd.Series([[]] * isna.sum()).values
    isna = big_df['education'].isna()
    big_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values

    return big_df.to_dict('records')

class PreprogramFunctions:
    def __init__(self, url):
        self.driver_instance = SeleniumDriver()
        self.driver_instance.start_driver(url,
                                          state_legislature_crawl_delay)

    def get_emails_as_dataframe(self):
        html = self.driver_instance.get_html_source()
        data_table = self.__extract_emails_table_as_df(html)
        return data_table[['Name', 'Email', 'District']]

    def __extract_emails_table_as_df(self, html):
        html_soup = soup(html, 'html.parser')
        html_email_table = html_soup.find('table', {'id': 'membertable'})
        table = pd.read_html(str(html_email_table))
        return table[0]

    def get_county_as_dataframe(self):
        html = self.driver_instance.get_html_source()
        data_table = self.__extract_county_table_as_df(html)
        return data_table[['Member', 'District', 'Counties']]
    
    def __extract_county_table_as_df(self, html):
        html_soup = soup(html, 'html.parser')
        html_email_table = html_soup.find('table', {'id': 'memberbydistrictandcountytable'})
        table = pd.read_html(str(html_email_table))
        return table[0]

class SoupMaker:
    def get_page_as_soup(self, url, crawl_delay):
        page_html = self.__get_site_as_html(url, crawl_delay)
        return soup(page_html, 'html.parser')

    def __get_site_as_html(self, url, crawl_delay):
        uClient = urlopen(url)
        page_html = uClient.read()
        uClient.close()
        scraper_utils.crawl_delay(crawl_delay)
        return page_html

class SeleniumDriver:
    def __init__(self):
        self.driver = webdriver.Chrome(ChromeDriverManager().install())
        self.driver.switch_to.default_content()  

    def start_driver(self, url, crawl_delay):
        try:
            self.driver.get(url)
            self.driver.maximize_window()
        except:
            error("Error opening the website.")
            self.close_driver()
        scraper_utils.crawl_delay(crawl_delay)
        sleep(5)

    def close_driver(self):
        self.driver.close()
        self.driver.quit()

    def get_html_source(self):
        try:
            html = self.driver.page_source
            return html
        except:
            error("Error in getting email table from selenium.")
        finally:
            self.close_driver()

class MainScraper:
    # For each member row
    # Look for div with class memberDetails
    # then look for divs with class col-csm-6 col-md-3 memberColumnPad
    # if the count of divs is 3, then get the first div and find the text of all the anchor tags
    # if it's 2, return current year
    def __init__(self, identity):
        self.identity = identity
        if identity == "Representative":
            url = REPRESENTATIVE_PAGE_URL
        else:
            url = SENATOR_PAGE_URL
        self.driver_instance = SeleniumDriver()
        self.driver_instance.start_driver(url, state_legislature_crawl_delay)
        try:
            self.data = self.__get_member_data()
        except Exception as e:
            print(e.with_traceback())
            error(f"Error getting {identity} member data.")
        finally:
            self.driver_instance.close_driver()

    def get_data(self):
        '''
        Returns a list of each member data (row)
        '''
        return self.data

    def __get_member_data(self):
        main_div = self.driver_instance.driver.find_element_by_id('memberList')
        members_web_element = main_div.find_elements_by_class_name('memberInformation')
        data = []
        for web_element in members_web_element:
            data.append(self.__set_data(web_element))
        return data

    def __set_data(self, member_web_element):
        row = scraper_utils.initialize_row()
        self.__set_name_data(row, member_web_element)
        self.__set_source_id(row, member_web_element)
        self.__set_role(row)
        self.__set_party_data(row, member_web_element)
        self.__set_district_and_county(row)
        self.__set_contact_info(row, member_web_element)
        self.__set_years_active(row, member_web_element)
        self.__set_most_recent_term_id(row)
        self.__set_source_url(row, member_web_element)
        self.__set_committee(row, member_web_element)
        self.__set_wiki_url(row)
        self.__set_gender(row)
        return row

    def __set_name_data(self, row, web_element):
        human_name = self.__get_name(web_element)
        row.name_full = human_name.full_name
        row.name_full = human_name.full_name
        row.name_last = human_name.last
        row.name_first = human_name.first
        row.name_middle = human_name.middle
        row.name_suffix = human_name.suffix

    def __get_name(self, web_element):
        name_container = web_element.find_element_by_class_name('memberName')
        name = self.__extract_name_from_container(name_container)
        return HumanName(name)

    def __extract_name_from_container(self, container):
        text = container.text
        text = text.split(self.identity)[1]
        text = text.split('(')[0]
        return text.strip()

    def __set_source_id(self, row, web_element):
        id_container = web_element.find_element_by_css_selector("div[class='col-csm-6 col-md-3 memberColumnPad']")
        a_link = id_container.find_element_by_tag_name('a')
        source_id = a_link.get_attribute('id')
        row.source_id = re.findall(r'\d+', source_id)[0]

    def __set_role(self, row):
        row.role = self.identity

    def __set_party_data(self, row, web_element):
        name_container = web_element.find_element_by_class_name('memberName')
        row.party = self.__extract_party_from_container(name_container)
        row.party_id = scraper_utils.get_party_id(row.party)

    def __extract_party_from_container(self, container):
        text = container.text
        text = text.split('(')[1]
        if 'R' in text:
            return 'Republican'
        elif 'D' in text:
            return 'Democrat'

    def __set_district_and_county(self, row):
        name_to_look_for = row.name_last + ', ' + row.name_first
        row.district = self.__set_district(name_to_look_for)
        row.areas_served = list(self.__set_county(name_to_look_for))

    def __set_district(self, name):
        data_row = every_county_as_df.loc[every_county_as_df['Member'].str.contains(name)]
        district = data_row['District'].values[0]
        district = district.split()[1].strip()
        return district

    def __set_county(self, name):
        data_row = every_county_as_df.loc[every_county_as_df['Member'].str.contains(name)]
        counties = data_row['Counties'].values[0]
        counties = counties.split('Counties ')[1]
        counties = counties.split(', ')
        return counties

    def __set_contact_info(self, row, web_element):
        member_information_div = web_element.find_element_by_css_selector("div[class='row clearfix']")
        member_main_columns = member_information_div.find_elements_by_css_selector("div[class='col-csm-6 col-md-3 memberColumnPad']")
        office_columns = member_main_columns[1:-1] # in between the first and last columns
        row.addresses = self.__get_addresses(office_columns)
        row.email = self.__get_email(row)
        row.phone_numbers = self.__get_numbers(office_columns)

    def __get_addresses(self, offices_web_element):
        addresses = []
        for office in offices_web_element:
            addresses.append(self.__extract_address(office))
        return addresses

    def __extract_address(self, office_web_element):
        office_name = office_web_element.find_element_by_class_name('memberColumnTitle').text
        address = office_web_element.find_element_by_tag_name('span').text
        address = self.__format_address(address)
        return {"location": office_name,
                "address": address}
        
    def __format_address(self, address):
        return address.replace('\n', ', ')

    def __get_email(self, row):
        '''
        Update Nov 3, 2021: Email's from website seemed to be taken down.
        Upon inspection, looks like emails are just a combination of name_first and name_last
        '''
        name_first = unidecode.unidecode(row.name_first.lower().replace(' ', ''))
        name_last = unidecode.unidecode(row.name_last.lower().replace(' ', ''))

        return name_first + '.' + name_last + '@leg.wa.gov'

    def __get_numbers(self, offices_web_element):
        numbers = []
        for office in offices_web_element:
            number = self.__extract_number(office)
            if number:
                numbers.append(number)
        return numbers
    
    def __extract_number(self, office_web_element):
        office_name = office_web_element.find_element_by_class_name('memberColumnTitle').text
        number = self.__find_number_from_text(office_web_element.text)
        if number:
            return {"office": office_name,
                    "number": number}
    
    def __find_number_from_text(self, text):
        try:
            number = re.findall(r'\([0-9]{3}\) [0-9]{3}-[0-9]{4}', text)[0]
        except:
            return
        return self.__format_number(number)

    def __format_number(self, number):
        return number.replace('(', '').replace(')', '').replace(' ', '-').strip()

    def __set_years_active(self, row, web_element):
        member_detail_container = web_element.find_element_by_class_name('memberDetails')
        details_container = member_detail_container.find_elements_by_css_selector("div[class='col-csm-6 col-md-3 memberColumnPad']")
        
        number_of_columns_when_voting_record_available = 3
        if len(details_container) < number_of_columns_when_voting_record_available:
            row.years_active = [CURRENT_YEAR]
        elif len(details_container) ==number_of_columns_when_voting_record_available:
            all_voting_years = details_container[0].find_elements_by_tag_name('a')
            row.years_active = self.__extract_voting_years(all_voting_years)

    def __extract_voting_years(self, years_web_element):
        years_active = []
        for year in years_web_element:
            self.__add_to_list_missing(years_active, year)
        years_active = list(set(years_active))
        years_active.sort()
        return(years_active)

    def __add_to_list_missing(self, return_list, year_web_element):
        year = year_web_element.get_attribute("text")
        year = re.findall(r'[0-9]{4}', year)[0]
        if year not in return_list:
            return_list.append(int(year))

    def __set_most_recent_term_id(self, row):
        if row.role == "Representative":
            self.__set_recent_term_for_representative(row)
        else:
            self.__set_recent_term_for_senator(row)
    
    def __set_recent_term_for_representative(self, row):
        if CURRENT_YEAR % 2 == 0:
            row.most_recent_term_id = str(CURRENT_YEAR)
        else:
            row.most_recent_term_id = str(CURRENT_YEAR - 1)

    def __set_recent_term_for_senator(self, row):
        '''
        Senates have a staggered election every four years.
        The last two elections (as of writing this code) was 2018 and 2020.
        '''
        districts_that_had_election_in_2020 = [
            1, 2, 3, 4, 5, 9, 10, 11, 12, 14, 16, 17, 18, 19, 20, 20, 22,
            23, 24, 25, 27, 28, 39, 40, 41, 49
        ]
        districts_that_had_election_in_2020 = set(districts_that_had_election_in_2020)

        if int(row.district) in districts_that_had_election_in_2020:
            row.most_recent_term_id = self.__calculate_most_recent_term_for_senator(2020)
        else:
            row.most_recent_term_id = self.__calculate_most_recent_term_for_senator(2018)
    
    def __calculate_most_recent_term_for_senator(self, last_election_recorded_in_code):
        year_difference = CURRENT_YEAR - last_election_recorded_in_code
        if year_difference % 4 == 0:
            return str(CURRENT_YEAR)
        else:
            return str(CURRENT_YEAR - (year_difference % 4))
        
    def __set_source_url(self, row, web_element):
        link = web_element.find_element_by_link_text('Home Page').get_attribute("href")
        row.source_url = link

    def __set_committee(self, row, web_element):
        member_information_div = web_element.find_element_by_css_selector("div[class='row clearfix']")
        member_main_columns = member_information_div.find_elements_by_css_selector("div[class='col-csm-6 col-md-3 memberColumnPad']")
        committees_web_element = member_main_columns[-1]
        row.committees = self.__extract_committee_data(committees_web_element)

    def __extract_committee_data(self, web_element):
        committees_container = web_element.find_elements_by_tag_name('span')
        return_list = []
        for committee in committees_container:
            self.__add_committee(return_list, committee)
        return return_list

    def __add_committee(self, return_list, committee_web_element):
        committee_info = committee_web_element.text
        committee_name = committee_info.split("(")[0].strip()
        roles = self.__get_role(committee_info)
        if len(roles) == 1:
            return_list.append({"role": roles[0],
                                "committee":committee_name})
        else:
            for role in roles:
                return_list.append({"role": role.strip(),
                                    "committee": committee_name})

    def __get_role(self, text):
        if '(' not in text:
            return ["Member"]
        role = text.split('(')[1].split(')')[0]
        roles = role.split(',')
        return roles

    def __set_wiki_url(self, row):

        if row.role == "Representative":
            try:
                page_soup = SoupMaker().get_page_as_soup(BALLOTPEDIA_REP, wiki_crawl_delay)
                tables = page_soup.findAll("table")
                rows = tables[3].findAll("tr")

                for person in rows[1:]:
                    tds = person.findAll("td")
                    name_td = tds[1]
                    name = name_td.text
                    name = name.replace('\n', '')
                    party = tds[2].text
                    party = party.strip()
                    party = party.replace('\n', '')
                    if party == "Democratic":
                        party = "Democrat"

                    try:
                        if row.party == party and row.name_last in name.strip() and name.strip().split(" ")[0] in row.name_first:
                            row.wiki_url = name_td.a['href']
                            break
                    except:
                        pass
                    if not row.wiki_url:
                        for person in rows[1:]:
                            tds = person.findAll("td")
                            name_td = tds[1]
                            name = name_td.text
                            name = name.replace('\n', '')
                            party = tds[2].text
                            party = party.strip()

                            if party == "Democratic":
                                party = "Democrat"

                            if row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                                row.wiki_url = name_td.a['href']
                                break
                            elif row.party == party and row.name_last in name.strip().split()[-1]:
                                row.wiki_url = name_td.a['href']
                                break
            except Exception as e:
                print(e)
        if row.role == "Senator":

            try:
                page_soup = SoupMaker().get_page_as_soup(BALLOTPEDIA_SEN, wiki_crawl_delay)
                tables = page_soup.findAll("table")
                rows = tables[3].findAll("tr")

                for person in rows[1:]:
                    tds = person.findAll("td")
                    name_td = tds[1]
                    name = name_td.text
                    name = name.replace('\n', '')
                    party = tds[2].text
                    party = party.strip()
                    party = party.replace('\n', '')
                    if party == "Democratic":
                        party = "Democrat"

                    try:
                        if row.party == party and row.name_last in name.strip() and name.strip().split(" ")[0] in row.name_first:
                            row.wiki_url = name_td.a['href']
                            break
                    except:
                        pass
                    if not row.wiki_url:
                        for person in rows[1:]:
                            tds = person.findAll("td")
                            name_td = tds[1]
                            name = name_td.text
                            name = name.replace('\n', '')
                            party = tds[2].text
                            party = party.strip()

                            if party == "Democratic":
                                party = "Democrat"

                            if row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                                row.wiki_url = name_td.a['href']
                                break
                            elif row.party == party and row.name_last in name.strip().split()[-1]:
                                row.wiki_url = name_td.a['href']
                                break
            except Exception as e:
                print(e)
                pass

    def __set_gender(self, row):
        gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
        if not gender:
            gender = 'O'
        row.gender = gender


class WikiScraper:
    def scrape_main_wiki_link(self, wiki_url):
        wiki_urls = []
        page_soup = SoupMaker().get_page_as_soup(wiki_url, wiki_crawl_delay)
        tables = page_soup.findAll("table")
        rows = tables[3].findAll("tr")
        for member in rows:
            info = member.findAll("td")
            try:
                biolink = info[1].a["href"]
                wiki_urls.append(biolink)
            except Exception as e:
                pass
        return wiki_urls


    def get_data_from_all_links(self, function, all_links):
        data = []
        with Pool(THREADS_FOR_POOL) as pool:
            data = pool.map(func=function,
                            iterable=all_links)
        return data

#global variable
# every_email_as_df = PreprogramFunctions(ALL_MEMBER_EMAIL_LIST_URL).get_emails_as_dataframe()
every_county_as_df = PreprogramFunctions(ALL_MEMBER_COUNTY_LIST_URL).get_county_as_dataframe()

if __name__ == '__main__':
    program_driver()
