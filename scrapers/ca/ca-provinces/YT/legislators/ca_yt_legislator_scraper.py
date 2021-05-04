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

def scrape_members_link(link_containing_all_members):
  mem_bios_urls = []
  uClient = urlopen(link_containing_all_members)
  page_html = uClient.read()
  uClient.close()

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

if __name__ == '__main__':
  pd.set_option('display.max_rows', None)
  pd.set_option('display.max_columns', None)
  mla_links = scrape_members_link(MLA_URL)

  print(mla_links)