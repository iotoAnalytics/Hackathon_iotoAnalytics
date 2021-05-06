import sys
import os
from pathlib import Path

# To find the required modules... but is it really required with venv?
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
import re
import datetime


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

scraper_utils = CAProvTerrLegislatorScraperUtils('YT', 'ca_yt_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

columns_not_on_main_site = ['birthday', 'education', 'occupation']

def scrape_members_link(main_link):
  mem_bios_urls = []
  page_html = get_site_as_html(main_link)
  page_soup = soup(page_html, 'html.parser')
  members_container = page_soup.find('div', {'class' : 'view-content'})
  member_container = members_container.findAll('span')

  for member in member_container:
    try:
      link_to_member_bio = BASE_URL + member.a['href']
      if link_to_member_bio not in mem_bios_urls:
        link_to_member_bio = link_to_member_bio.replace('\n', '')
        mem_bios_urls.append(link_to_member_bio)
    except Exception:
      pass
  scraper_utils.crawl_delay(crawl_delay)
  return mem_bios_urls

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


# !!!Want to add this to general functoins
def remove_prefix_from_name(full_name, prefix):
  return full_name.replace(prefix, "").strip()


def assign_role(role):
  if role == None:
    return "Member of the Legislative Assembly"
  else:
    return role.text

# Repeated so I wanted to extract as function
def get_site_as_html(link_to_open):
  uClient = urlopen(link_to_open)
  page_html = uClient.read()
  uClient.close()
  return page_html

if __name__ == '__main__':
  pd.set_option('display.max_rows', None)
  pd.set_option('display.max_columns', None)
  individual_mla_links = scrape_members_link(MLA_URL)

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