## TODO: Raise Exception when looking for element and it's not there

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
import requests
from multiprocessing import Pool
from nameparser import HumanName
import pandas as pd
import unidecode
import numpy as np
from tqdm import tqdm

BASE_URL = 'https://yukonassembly.ca/'
MLA_URL = 'https://yukonassembly.ca/mlas?field_party_affiliation_target_id=All&field_assembly_target_id=All&sort_by=field_last_name_value'
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
columns_not_on_main_site = ['birthday', 'education', 'occupation']

scraper_utils = CAProvTerrLegislatorScraperUtils('YT', 'ca_yt_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def driver():
  main_page_soup = get_page_as_soup(MLA_URL)
  scraper_for_main = ScraperForMainSite()

  print("Getting data from mla pages...")
  all_mla_links = scraper_for_main.get_all_mla_links(main_page_soup)
  mla_data = get_data_from_all_links(get_mla_data, all_mla_links)

  print("Getting data from wiki pages...")
  individual_wiki_links = scrape_main_wiki_link(WIKI_URL)
  wiki_data = get_data_from_all_links(scraper_utils.scrape_wiki_bio, individual_wiki_links)

  complete_data_set = configure_data(mla_data, wiki_data)

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
  return page_html

def get_data_from_all_links(function, all_mla_links):
  with Pool() as pool:
    data = pool.map(func=function,
                    iterable=all_mla_links)
  return data

def get_mla_data(mla_url):
  scraper_for_mla = ScraperForMLAs(mla_url)
  return scraper_for_mla.get_rows()

def configure_data(mla_data, wiki_data):
  mla_df = pd.DataFrame(mla_data)
  mla_df = mla_df.drop(columns = columns_not_on_main_site)
  
  wiki_df = pd.DataFrame(wiki_data)[
    ['birthday', 'education', 'name_first', 'name_last', 'occupation']
  ]

  big_df = pd.merge(mla_df, wiki_df, 
                    how='left',
                    on=['name_first', 'name_last'])
  big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
  big_df['occupation'] = big_df['occupation'].replace({np.nan: None})
  big_df['education'] = big_df['education'].replace({np.nan: None})

  return big_df.to_dict('records')

def scrape_main_wiki_link(wiki_link):
    wiki_urls = []
    page_html = get_site_as_html(wiki_link)
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    table = page_soup.find("table", {"class": "wikitable sortable"})
    table = table.findAll("tr")[1:]
    for tr in table:
        td = tr.findAll("td")[1]
        url = 'https://en.wikipedia.org' + (td.a["href"])

        wiki_urls.append(url)
    scraper_utils.crawl_delay(crawl_delay)
    return wiki_urls

class ScraperForMainSite:  
  def get_all_mla_links(self, main_page_soup):
    mem_bios_urls = []
    list_of_url_spans = self.__get_list_of_member_url_span(main_page_soup)

    for span in list_of_url_spans:
      self.__extract_mla_url_from_span(span, mem_bios_urls)
    scraper_utils.crawl_delay(crawl_delay)
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
    self.set_row_data()

  def get_rows(self):
    return self.row

  def set_row_data(self):
    self.row.source_url = self.url
    self.set_name_data()
    self.set_role_data()
    self.set_party_data()
    self.set_riding_data()
    # Everyone has the same office address. That address is declared at the footer of page
    self.set_address()
    self.set_contact_info()
    self.set_service_period()
    self.set_most_recent_term_id()

  def set_name_data(self):
    human_name = self.get_full_human_name()
    self.row.name_full = human_name.full_name
    self.row.name_last = human_name.last
    self.row.name_first = human_name.first
    self.row.name_middle = human_name.middle
    self.row.name_suffix = human_name.suffix

  def get_full_human_name(self):
    full_name = self.main_container.find('span').text
    full_name = full_name.replace('hon', '').strip()
    return HumanName(full_name)
  
  def set_role_data(self):
    role_container = self.main_container.find('div', {'class' : 'field--name-field-title'})
    self.row.role = self.assign_role(role_container)

  def assign_role(self, role):
    if role == None:
      return "Member of the Legislative Assembly"
    else:
      return role.text

  def set_party_data(self):
    party_info_container = self.main_container.find('div', {'class' : 'field--name-field-party-affiliation'})
    party_info = party_info_container.text
    party_name = self.edit_party_name(party_info)
    self.row.party = party_name
    self.set_party_id(party_name)
  
  def edit_party_name(self, party_name):
    if 'Liberal' in party_name:
      return 'Liberal'
    elif 'Democratic' in party_name:
      return 'New Democratic'
    else:
      return party_name
  
  def set_party_id(self, party_name):
    try:
      self.row.party_id = scraper_utils.get_party_id(party_name)
    except:
      row.party_id = 0

  def set_riding_data(self):
    riding = self.main_container.find('div', {'class' : 'field--name-field-constituency'}).text
    self.row.riding = riding

  def set_address(self):
    full_address = self.get_address()
    address_location = full_address[0]
    street_address = full_address[1] + full_address[2]
    address_info = {'location' :  address_location, 'address' : street_address}
    self.row.addresses = address_info
  
  '''
  This function returns part of a list (parts_of_address).
  This is because only the first three from the split address is relevant.
  If html structure changes, this may need to be fixed.
  '''
  def get_address(self):
    page_footer = self.soup.find('footer')
    address_container = page_footer.find('div', {'class' : 'footer-row--right'})
    full_address = address_container.find('p').text
    full_address = full_address.replace('\xa0', '')
    parts_of_address = full_address.split('\n')
    return parts_of_address[:3]

  def set_contact_info(self):
    contact_info = self.get_contact_info()
    self.row.email = contact_info.a.text
    self.row.phone_numbers = self.get_phone_numbers(contact_info)

  def get_contact_info(self):
    profile_sidebar = self.soup.find('aside', {'class' : 'member-sidebar'})
    contact_info_container = profile_sidebar.find('article')
    return contact_info_container.findAll('p')[1]
  
  def get_phone_numbers(self, contact_info):
    numbers = re.findall(r'[0-9]{3}-[0-9]{3}-[0-9]{4}', contact_info.text)
    return self.categorize_numbers(numbers)

  '''
  phone_types is in this order because the website has the numbers ordered from phone then fax,
  so effectively our numbers parm from above method will store numbers as such
  '''
  def categorize_numbers(self, numbers):
    categorized_numbers = []
    i = 0
    phone_types = ['phone', 'fax']
    for number in numbers:
      info = {'office' : phone_types[i],
              'number' : number}
      categorized_numbers.append(info)
    i = i + 1
    return categorized_numbers

  def set_service_period(self):
    in_service_paragraph = self.get_service_paragraph()
    service_periods = self.get_service_periods(in_service_paragraph)
    self.row.years_active = service_periods
  
  def get_service_paragraph(self):
    mla_summary_container = self.soup.find('div', {'class' : 'field--type-text-with-summary'})
    mla_summary_paragraphs = mla_summary_container.findAll('p')
    return self.find_key_sentence_from_paragraph(mla_summary_paragraphs)

  def find_key_sentence_from_paragraph(self, paragraphs):
    for paragraph in paragraphs:
      if 'elected to the Yukon Legislative Assembly' in paragraph.text:
        return paragraph.text

  def get_service_periods(self, paragraph):
    service_periods_as_string = re.findall('\d\d[a-z]{2}', paragraph)
    service_periods_as_int = [int(period[0:2]) for period in service_periods_as_string]
    return self.get_service_periods_as_years(service_periods_as_int)

  def get_service_periods_as_years(self, service_periods_as_int):
    service_periods_as_years = []
    for period in service_periods_as_int:
      self.add_periods_as_years(period, service_periods_as_years)
    return service_periods_as_years
      
  def add_periods_as_years(self, period, return_list):
    last_period = list(NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR)[-1]
    current_term_year = NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR.get(period)
    if period != last_period:
      next_term_year = NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR.get(period + 1)
      for i in range(current_term_year, next_term_year):
        return_list.append(i)
    elif CURRENT_YEAR > period:
      for i in range(current_term_year, CURRENT_YEAR + 1):
        return_list.append(i)

  def set_most_recent_term_id(self):
    self.row.most_recent_term_id = self.row.years_active[-1]

if __name__ == '__main__':
  driver()