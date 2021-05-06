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

def driver():
  main_page_soup = get_page_as_soup(MLA_URL)
  scraper_for_main = ScraperForMainSite()

  all_mla_links = scraper_for_main.get_all_mla_links(main_page_soup)

  with Pool() as pool:
    data = pool.map(func=get_mla_data,
                    iterable=all_mla_links)

  # delete after... only here because need to test if main function at the end runs
  return all_mla_links

def get_page_as_soup(url):
  page_html = get_site_as_html(url)
  return soup(page_html, 'html.parser')

def get_mla_data(mla_url):
  scraper_for_mla = ScraperForMLAs(mla_url)
  return scraper_for_mla.get_rows()

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

class ScraperForMLAs():
  def __init__(self, mla_url):
    self.row = scraper_utils.initialize_row()
    self.url = mla_url
    self.main_container = find_main_container()
    self.set_row_data()

  def find_main_container():
    page_soup = get_page_as_soup(self.url)
    return page_soup.find('div', {'class' : 'content'})

  def get_rows():
    return self.row

  def set_row_data():
    self.row.source_url = self.url
    set_name_data()
    set_role_data()
    set_party_data()

  def set_name_data():
    human_name = get_full_human_name()
    row.name_full = human_name.full_name
    row.name_last = human_name.last
    row.name_first = human_name.first
    row.name_middle = human_name.middle
    row.name_suffix = human_name.suffix

  def get_full_human_name():
    full_name = self.main_container.find('span'.text)
    full_name = full_name.replace('hon', '').strip()
    return HumanName(full_name)
  
  def set_role_data():
    role_container = self.main_container.find('div', {'class' : 'field--name-field-title'})
    row.role = assign_role(role_containre)

  def assign_role(role):
    if role == None:
      return "Member of the Legislative Assembly"
    else:
      return role.text

  def set_party_data():
    party_info_container = self.main_container.find('div', {'class' : 'field--name-field-party-affiliation'})
    party_info = party_info_container.text
    party_name = edit_party_name(party_info)
    row.party = party_name
  
  def edit_party_name(party_name):
    if 'Liberal' in party_name:
      return 'Liberal'
    elif 'Democratic' in party_name:
      return 'New Democratic'
    else:
      return party_name

# TODO: CLEAN UP CODE
# Try and refactor each part to a separate function(like get name, add name data)
def collect_mla_data(link_to_mla):
  row = scraper_utils.initialize_row()
  page_html = get_site_as_html(link_to_mla)
  page_soup = soup(page_html, 'html.parser')

  row.source_url = link_to_mla

  main_content_container = page_soup.find('div', {'class' : 'content'})
  full_name = main_content_container.find('span').text
  full_name = remove_prefix_from_name(full_name, "Hon.")
  
  human_name = HumanName(full_name)

  row.name_full = human_name.full_name
  row.name_last = human_name.last
  row.name_first = human_name.first
  row.name_middle = human_name.middle
  row.name_suffix = human_name.suffix

  role_container = main_content_container.find('div', {'class' : 'field--name-field-title'})
  row.role = assign_role(role_container)

  party_info = main_content_container.find('div', {'class' : 'field--name-field-party-affiliation'}).text
  
  if 'Liberal' in party_info:
    party_info = 'Liberal'
  elif 'Democratic' in party_info:
    party_info = 'New Democratic'
  
  row.party = party_info
  try:
    row.party_id = scraper_utils.get_party_id(party_info)
  except:
    row.party_id = 0

  riding = main_content_container.find('div', {'class' : 'field--name-field-constituency'}).text
  row.riding = riding

  # Everyone has the same office address. That address is declared at the footer of page
  page_footer = page_soup.find('footer')
  address_container = page_footer.find('div', {'class' : 'footer-row--right'})
  full_address = address_container.find('p').text
  full_address = full_address.replace('\xa0', '')
  parts_of_address = full_address.split('\n')

  address_location = parts_of_address[0]

  # This may not make sense but the address that is split has 6 entries, last two are not relevant
  address = parts_of_address[1] + parts_of_address[2]
  address_info = {'location' :  address_location, 'address' : address}
  row.addresses = [address_info]

  member_sidebar = page_soup.find('aside', {'class' : 'member-sidebar'})
  contact_info_container = member_sidebar.find('article')
  contact_info = contact_info_container.findAll('p')[1]

  email_address = contact_info.a.text
  row.email = email_address

  phone_number = re.findall(r'[0-9]{3}-[0-9]{3}-[0-9]{4}', contact_info.text)
  phone_info = []
  i = 0
  phone_types = ['Phone', 'Fax']
  for number in phone_number:
    info = {'office' : phone_types[i],
                  'number' : number}
    phone_info.append(info)
    i = i + 1
  row.phone_numbers = phone_info

  mla_summary_container = page_soup.find('div', {'class' : 'field--type-text-with-summary'})
  mla_summary_paragraphs = mla_summary_container.findAll('p')
  
  in_service_paragraph = ''
  for paragraph in mla_summary_paragraphs:
    if 'elected to the Yukon Legislative Assembly' in paragraph.text:
      in_service_paragraph = paragraph.text
     
  service_periods = re.findall('\d\d[a-z]{2}', in_service_paragraph)
  service_periods_as_int = []
  for period in service_periods:
    service_periods_as_int.append(int(period[0:2]))

  service_periods_as_years = []
  last_period = list(NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR)[-1]
  for period in service_periods_as_int:
    current_term_year = NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR.get(period)
    if period != last_period:
      next_period = period + 1
      next_term_year = NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR.get(next_period)
      for i in range(current_term_year, next_term_year):
        service_periods_as_years.append(i)
    elif CURRENT_YEAR > period:
      for i in range(current_term_year, CURRENT_YEAR + 1):
        service_periods_as_years.append(i)

  row.years_active = service_periods_as_years

  longest_service_period = find_longest_thread(service_periods_as_int)
  most_recent_term = longest_service_period[-1]

  row.most_recent_term_id = NTH_LEGISLATIVE_ASSEMBLY_TO_YEAR.get(most_recent_term)

  scraper_utils.crawl_delay(crawl_delay)
  return row

# !!!Might want to add this to general functions
def find_longest_thread(array_of_ints):
  if len(array_of_ints) == 1:
    return array_of_ints
  
  return_array = [array_of_ints[0]]
  for i in range(1, len(array_of_ints)):
    if array_of_ints[i] == return_array[-1] + 1:
      return_array.append(array_of_ints[i])
    else:
      return_array.clear()
      return_array.append(array_of_ints[i])
  return return_array

# Repeated so I wanted to extract as function
def get_site_as_html(link_to_open):
  uClient = urlopen(link_to_open)
  page_html = uClient.read()
  uClient.close()
  return page_html

if __name__ == '__main__':
  pd.set_option('display.max_rows', None)
  pd.set_option('display.max_columns', None)
  individual_mla_links = driver()

  with Pool() as pool:
    data = pool.map(func=collect_mla_data, iterable=individual_mla_links)
  
  mla_df = pd.DataFrame(data)
  mla_df = mla_df.drop(columns = columns_not_on_main_site)

  individual_wiki_links = scrape_main_wiki_link(WIKI_URL)
  with Pool() as pool:
    wiki_data = pool.map(func=scraper_utils.scrape_wiki_bio, iterable=individual_wiki_links)
  wiki_df = pd.DataFrame(wiki_data)[
      ['birthday', 'education', 'name_first', 'name_last', 'occupation']]

  big_df = pd.merge(mla_df, wiki_df, how='left',
                    on=['name_first', 'name_last'])
  big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
  big_df['occupation'] = big_df['occupation'].replace({np.nan: None})
  big_df['education'] = big_df['education'].replace({np.nan: None})
  
  big_list_of_dicts = big_df.to_dict('records')
  print('Writing data to database...')

  scraper_utils.write_data(big_list_of_dicts)

  print('Complete!')