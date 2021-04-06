import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))
import io
from legislation_scraper_utils import CAProvinceTerrLegislationScraperUtils
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import PyPDF2
import urllib.parse as urlparse
from urllib.parse import parse_qs
from pprint import pprint
import datetime
import boto3
from urllib.request import urlopen as uReq
from urllib.request import Request
from bs4 import BeautifulSoup as soup
import pandas as pd

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

prov_terr_abbreviation = 'AB'
database_table_name = 'ca_ab_legislation'
legislator_table_name = 'ca_ab_legislators'

scraper_utils = CAProvinceTerrLegislationScraperUtils(prov_terr_abbreviation,
                                                      database_table_name,
                                                      legislator_table_name)


def scrape_bill_links(link):
    bill_links = []
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    bill_items = page_soup.findAll("div", {"class": "item billgt"})
    for bi in bill_items:
        url = 'https://www.assembly.ab.ca/' + bi.div.a["href"]

        bill_links.append(url)
    private_member_bills = page_soup.findAll("div", {"class": "item billpb"})
    for bi in private_member_bills:
        url = 'https://www.assembly.ab.ca/' + bi.div.a["href"]

        bill_links.append(url)
    private_bills = page_soup.findAll("div", {"class": "item billpr"})
    for bi in private_bills:
        url = 'https://www.assembly.ab.ca/' + bi.div.a["href"]

        bill_links.append(url)
    return bill_links


def scrape_bills(link):
    print(link)
    row = scraper_utils.initialize_row()
    row.source_url = link
    row.chamber_origin = 'Legislative Assembly'
    bill_id = link.split("infoid=")[1]
    bill_id = bill_id.split('&')[0]
    print(bill_id)
    row.source_id = bill_id
    # get goverlytics_id, url
    row.goverlytics_id = "QC_2018_" + bill_id
    # url = "/ca/QC/legislation/" + goverlytics_id

    return row


if __name__ == '__main__':
    bills_main = 'https://www.assembly.ab.ca/assembly-business/bills/bills-by-legislature'
    bill_links = scrape_bill_links(bills_main)
    less_links = bill_links[:10]
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    with Pool() as pool:
        bill_data = pool.map(func=scrape_bills, iterable=bill_links)
    bill_df = pd.DataFrame(bill_data)
    print(bill_df)

    big_list_of_dicts = bill_df.to_dict('records')
    # print(*big_list_of_dicts, sep="\n")

    print('Writing data to database...')
    scraper_utils.insert_legislation_data_into_db(big_list_of_dicts)

    print('Complete!')

