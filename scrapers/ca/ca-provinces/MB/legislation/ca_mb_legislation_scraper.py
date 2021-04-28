import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

import io
from scraper_utils import CAProvinceTerrLegislationScraperUtils
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
import datetime
import boto3
from urllib.request import urlopen as uReq
from urllib.request import Request
from bs4 import BeautifulSoup as soup
import pandas as pd


prov_terr_abbreviation = 'MB'
database_table_name = 'ca_mb_legislation'
legislator_table_name = 'ca_mb_legislators'

scraper_utils = CAProvinceTerrLegislationScraperUtils(prov_terr_abbreviation,
                                                      database_table_name,
                                                      legislator_table_name)
crawl_delay = scraper_utils.get_crawl_delay('https://web2.gov.mb.ca/')


def scrape_bill_links(link):
    bill_infos = []
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    tables = page_soup.findAll('table', {'class': 'index'})

    for table in tables:
        trs = table.findAll("tr")[4:]
        for tr in trs:
            td = tr.find("td", {'class': "left sm"})

            source_url = 'https://web2.gov.mb.ca/bills/42-3/' + td.a["href"]

            num = tr.find("td", {"class": "right sm"}).text

            sponsor = tr.find("td", {"class": "left sm8"}).text
            sponsor = sponsor.split(".")
            comm = sponsor[len(sponsor) - 1]

            sponsor = sponsor[len(sponsor) - 1]
            sponsor = sponsor.split("Minister")[0]
            comm = comm.split(sponsor)[1]

            sponsor = sponsor.replace("MLA", "").strip()
            committees = []
            if "of" in comm:
                committee = comm.split("of")[1].strip()
                com_info = {'chamber': 'Legislative Assembly',
                            'committee': committee}
                committees.append(com_info)
            elif "for the" in comm:
                committee = comm.split("for the")[1].strip()
                com_info = {'chamber': 'Legislative Assembly',
                            'committee': committee}
                committees.append(com_info)

            search_for = dict(name_last=sponsor)
            try:

                s_id = scraper_utils.get_legislator_id(**search_for)
                s_id = int(s_id)

                principal_sponsor_id = (s_id)

            except:

                principal_sponsor_id = 0

            title = tr.find("td", {"class": "left sm"}).text
            title = title.split('\n')[0]

            info = {'source_url': source_url, 'bill_name': "Bill-" + num, 'principal_sponsor': sponsor,
                    'principal_sponsor_id': principal_sponsor_id, 'bill_title': title, 'committees': committees}

            bill_infos.append(info)
    scraper_utils.crawl_delay(crawl_delay)
    return bill_infos


def collect_bill_data(info):
    link = info['source_url']

    row = scraper_utils.initialize_row()
    session = link.split("bills/")[1]
    row.session = session.split("/")[0]
    source_id = link.split("/")
    source_id = source_id[len(source_id)-1]
    source_id = source_id.split(".")[0]
    row.source_id = source_id

    row.source_url = link
    row.bill_name = info['bill_name']
    row.principal_sponsor = info['principal_sponsor']
    row.principal_sponsor_id = info['principal_sponsor_id']
    row.bill_title = info['bill_title']
    row.committees = info['committees']
    row.goverlytics_id = row.province_territory + \
        '_' + row.session + '_' + row.bill_name

    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    try:
        xnote = page_soup.find("table", {"class": "xnote"})
        explan = xnote.findAll("p")[1]
        explan = explan.text.strip()
        explan = explan.replace('\n', "")
        row.bill_description = explan
        # print(row.bill_description)
    except:
        # print(link)
        pass
    row.chamber_origin = 'Legislative Assembly'
    row.bill_type = 'Bill'
    centers = page_soup.find("table", {"class": "centers"})
    center = centers.find("td", {"class": "center"})
    pdf_link = 'https://web2.gov.mb.ca/bills/42-3/' + (center.a["href"])
    bill_text = ""

    try:
        r = scraper_utils.request(pdf_link)
        f = io.BytesIO(r.content)
        reader = PyPDF2.PdfFileReader(f, strict=False)
        if reader.isEncrypted:
            reader.decrypt('')
        page_done = 0
        i = 0
        while page_done == 0:
            try:
                contents = reader.getPage(i).extractText()
                bill_text = bill_text + " " + contents

            except:
                page_done = 1
            i = i+1
        bill_text = bill_text.replace("\n", "")

    except:

        pass
    row.bill_text = bill_text
    scraper_utils.crawl_delay(crawl_delay)
    return row


if __name__ == '__main__':
    bills_main = 'https://web2.gov.mb.ca/bills/42-3/index.php'
    bill_infos = scrape_bill_links(bills_main)
    less_infos = bill_infos[:70]
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    with Pool() as pool:
        data = pool.map(func=collect_bill_data, iterable=bill_infos)
    bill_df = pd.DataFrame(data)
    bill_df = scraper_utils.add_topics(bill_df)
    print(bill_df)

    big_list_of_dicts = bill_df.to_dict('records')
    print(*big_list_of_dicts, sep="\n")

    print('Writing data to database...')
    scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
