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
import datetime
import boto3
from urllib.request import urlopen as uReq
from urllib.request import Request
from bs4 import BeautifulSoup as soup
import pandas as pd
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))


# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

prov_terr_abbreviation = 'AB'
database_table_name = 'ca_ab_legislation'
legislator_table_name = 'ca_ab_legislators'

scraper_utils = CAProvinceTerrLegislationScraperUtils(prov_terr_abbreviation,
                                                      database_table_name,
                                                      legislator_table_name)

crawl_delay = scraper_utils.get_crawl_delay('https://www.assembly.ab.ca/')


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

    scraper_utils.crawl_delay(crawl_delay)
    return bill_links


def scrape_bills(link):

    row = scraper_utils.initialize_row()
    row.source_url = link
    row.chamber_origin = 'Legislative Assembly'
    bill_id = link.split("infoid=")[1]
    bill_id = bill_id.split('&')[0]

    row.source_id = bill_id

    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    details = page_soup.findAll("div", {"class": "detail"})
    bill_name_details = (details[0]).findAll("div")
    bill_name = bill_name_details[1].text
    row.bill_name = bill_name.replace(u'\xa0', "")

    bill_title_details = (details[1].findAll("div"))
    bill_title = bill_title_details[1].text
    row.bill_title = bill_title

    session_details = (details[2]).findAll("div")
    session = session_details[1].text
    row.session = session.split(",")[1].strip()

    row.goverlytics_id = "AB_" + \
        row.session.replace(" ", "") + '_' + row.bill_name

    sponsor_details = (details[3]).findAll("div")
    prin_sponsor = sponsor_details[1].text
    row.principal_sponsor = prin_sponsor

    search_for = dict(name_last=prin_sponsor)
    try:

        s_id = scraper_utils.get_legislator_id(**search_for)
        s_id = int(s_id)

        principal_sponsor_id = (s_id)

    except:

        principal_sponsor_id = 0
    row.principal_sponsor_id = principal_sponsor_id

    type_details = (details[4]).findAll("div")
    bill_type = type_details[1].text
    row.bill_type = bill_type

    pdf_details = (details[7]).findAll("div")
    # print(pdf_details)

    bill_pdf = pdf_details[1].a["href"]
    # print(bill_pdf)
    bill_text = ""

    try:
        r = scraper_utils.request(bill_pdf)
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
            i = i + 1
        bill_text = bill_text.replace("\n", "")

    except:

        pass
    row.bill_text = bill_text
    # print(bill_text)

    bill_entries = page_soup.find("div", {"class": "bill-entries"})
    first = page_soup.find("div", {"class": "b_entry"})
    date_introduced = first.div.div.span.text
    date_introduced = date_introduced.split(" ")
    date_introduced = " ".join(date_introduced[:3])
    # print(date_introduced)
    d = datetime.datetime.strptime(date_introduced, '%b %d, %Y')
    d = d.strftime('%Y-%m-%d')
    row.date_introduced = d

    bill_entries = bill_entries.findAll("div")
    actions = []
    action_info = page_soup.findAll(True, {'class': ['b_header', 'b_entry']})
    for a in action_info:
        if (a["class"]) == ['b_header']:
            header = a.text.strip()
        elif (a["class"]) == ['b_entry']:
            try:
                status = a.find("div", {"class": "b_status"}).text.strip()

            except:
                status = ""
            description = header + " " + status
            description = description.strip()
            action_date = a.div.div.span.text.split(" ")
            action_date = " ".join(action_date[:3])
            try:
                ad = datetime.datetime.strptime(action_date, '%b %d, %Y')
                ad = ad.strftime('%Y-%m-%d')

            except:
                ad = None
            action = {'date': ad, 'action_by': 'Legislative Assembly',
                      'description': description.replace('\n', " ")}
            # print(action)
            actions.append(action)
    # print(actions)

    actions.reverse()
    row.actions = actions
    current_status = actions[0]['description']
    row.current_status = current_status
    # print(row.actions)

    # for div in bill_entries:
    scraper_utils.crawl_delay(crawl_delay)
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
    print(*big_list_of_dicts, sep="\n")

    print('Writing data to database...')
    scraper_utils.insert_legislation_data_into_db(big_list_of_dicts)

    print('Complete!')
