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


BASE_URL = 'https://yukonassembly.ca/'
MLA_URL = 'https://yukonassembly.ca/mlas?field_party_affiliation_target_id=All&field_assembly_target_id=All&sort_by=field_last_name_value'
DIV_CONTAINING_MEMBERS = {'class' : 'view-content'}

scraper_utils = CAProvTerrLegislatorScraperUtils('YT', 'ca_yt_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

columns_not_on_main_site = ['years_active']

def scrape_members_link(link_containing_all_members):
  mem_bios_urls = []
  page_html = get_site_as_html(link_containing_all_members)
  page_soup = soup(page_html, 'html.parser')
  members_container = page_soup.find('div', DIV_CONTAINING_MEMBERS)
  member_container = members_container.findAll('span')

  for member in member_container:
    try:
      link_to_member_bio = BASE_URL + member.a['href']
      if link_to_member_bio not in mem_bios_urls:
        mem_bios_urls.append(link_to_member_bio)
    except Exception:
      pass
  scraper_utils.crawl_delay(crawl_delay)
  return mem_bios_urls

# Try and refactor each part to a separate function(like get name, add name data)
def collect_mla_data(link_to_mla):
  row = scraper_utils.initialize_row()
  page_html = get_site_as_html(link_to_mla)
  page_soup = soup(page_html, 'html.parser')

  row.source_url = link_to_mla

  main_content_container = page_soup.find('div', {'class' : 'content'})
  full_name = main_content_container.find('span').text
  full_name = remove_prefix_from_name(full_name, "Hon.")

  row.name_full = human_name.full_name
  row.name_last = human_name.last
  row.name_first = human_name.first
  row.name_middle = human_name.middle
  row.name_suffix = human_name.suffix

  role_container = main_content_container.find('div', {'class' : 'field--name-field-title'})
  row.role = assign_role(role_container)

  party_info = main_content_container.find('div', {'class' : 'field--name-field-party-affiliation'}).text
  row.party = party_info
  try:
    row.party_id = scraper_utils.get_party_id(party_info)
  except:
    row.party_id = 0

  riding = main_content_container.find('div', {'class' : 'field--name-field-constituency'}).text
  row.riding = riding
  return row

# Want to add this to general functoins
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
  mla_links = scrape_members_link(MLA_URL)

  with Pool() as pool:
    data = pool.map(func=collect_mla_data, iterable=mla_links)
  
  leg_df = pd.DataFrame(data)
  leg_df = leg_df.drop(columns = columns_not_on_main_site)
  print(leg_df)