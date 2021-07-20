import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

from tqdm import tqdm
import io
from nameparser import HumanName
import json
import psycopg2
import csv
from datetime import datetime, date
import xml.etree.ElementTree as ET
import requests
from scraper_utils import CAFedLegislationScraperUtils
from bs4 import BeautifulSoup


base_url = 'https://www.parl.ca'
xml_url_csv = 'xml_urls.csv'
table_name = 'ca_federal_legislation'

scraper_utils = CAFedLegislationScraperUtils()
crawl_delay = scraper_utils.get_crawl_delay(base_url)
