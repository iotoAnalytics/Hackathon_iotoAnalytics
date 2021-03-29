import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))
import json
import os
import pickle
import numpy as np
import gzip
import argparse
import time
import unicodedata
import matplotlib.pyplot as plt
from numpy.linalg import norm
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from sklearn.decomposition import PCA
import utils
import pandas as pd
import bs4
from urllib.request import urlopen as uReq
from urllib.request import Request
from bs4 import BeautifulSoup as soup
import psycopg2
from nameparser import HumanName
import requests
import datefinder
import unidecode
from multiprocessing import Pool
import datetime
import re
import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
# import dbwork
import PyPDF2
import requests
import io
# import xmltodict
import requests
import xml.etree.ElementTree as ET
from legislation_scraper_utils import USStateLegislationScraperUtils

state_abbreviation = 'AZ'
database_table_name = 'us_az_legislation'
legislator_table_name = 'us_az_legislators'

scraper_utils = USStateLegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)



def find_bill_links(myurl):
    bill_links = []
    req = Request(myurl,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()

    uReq(req).close()

    page_soup = soup(webpage, "html.parser")
    tables_body = page_soup.findAll("tbody")
    for tb in tables_body:
        lines = tb.findAll("a")
        for l in lines:
            bill_link = (l["href"])

            session = bill_link.split("SessionId=")[1]

            bill_state_id = bill_link.split("w/")[1]
            bill_state_id = bill_state_id.split("?")[0]

            bill_name = l.text

            if "S" in bill_name:
                chamber_origin = "Senate"
            if "H" in bill_name:
                chamber_origin = "House"
            else:
                chamber_origin = ""

            bill_type = "Other"
            if "R" in bill_name:
                bill_type = "Resolution"
            elif "B" in bill_name:
                bill_type = "Bill"

            # get goverlytics_id, url
            goverlytics_id = "AZ_2019-2020_" + bill_name
            url = "/us/AZ/legislation/" + goverlytics_id

            info = {'source_url': bill_link, 'bill_name': bill_name, 'chamber_origin': chamber_origin,
                    'bill_type': bill_type, 'goverlytics_id': goverlytics_id, 'url': url,
                    'source_id': bill_state_id, 'session': session}
            # print(info)
            bill_links.append(info)

    return bill_links


def collect_bill_data(myurl):
    principal_sponsor = ""
    cosponsors = []
    sponsors = []
    cosponsors_id = []
    sponsors_id = []
    principal_sponsor_id = None
    # done = 0
    # tries = 0
    # while done == 0:
    #
    #     try:
    #         tries += 1
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome('chromedriver.exe', chrome_options=chrome_options)
    wait = WebDriverWait(driver, 10)

    driver.get(myurl)
    driver.switch_to.default_content()
    #         divs = driver.find_elements_by_id('billStatusSearchDiv')
    #         bill_html = divs[0].get_attribute('innerHTML')
    #         bill_soup = soup(bill_html, 'lxml')
    #         driver.quit()
    #         slist = bill_soup.find('select', {'id': 'slist'})
    #
    #         sponsor_list = slist.text.split("\n")
    #
    #         for sponsor in sponsor_list:
    #             if sponsor != "":
    #                 if "(Prime)" in sponsor:
    #                     principal_sponsor = str(sponsor.replace("(Prime)", "").strip())
    #                 elif "(Co-Sponsor)" in sponsor:
    #                     cosponsors.append(str(sponsor.replace("(Co-Sponsor)", "").strip()))
    #                 else:
    #                     sponsors.append(str(sponsor.split("(")[0].strip()))
    #         done = 1
    #
    #
    #
    #     except:
    #         if tries == 3:
    #             done = 1
    #             return {'source_url': myurl}
    #         # print(myurl)
    #         tries += 1

    driver.get(myurl)
    timeout = 5

    try:
        element_present = EC.presence_of_element_located((By.CLASS_NAME, 'row content-large'))
        WebDriverWait(driver, timeout).until(element_present)


    except:
        # print("timeout")
        return {'source_url': myurl, 'bill_title': "",
                 'current_status': "", 'state': 'AZ', 'state_id': 4, 'topic': "",
                 'date_introduced': None, 'actions': [], 'committees': [],
                 'bill_description': "", 'bill_summary': "", 'votes': []}

    html = driver.page_source
    bill_soup = soup(html, 'html.parser')



    #     if sponsor in dbwork.pslastnames:
    #         suindex = dbwork.pslastnames.index(sponsor)
    #         # print(sponsor)
    #         if dbwork.psids[suindex] not in cosponsors_id:
    #             cosponsors_id.append(dbwork.psids[suindex])
    # for sponsor in cosponsors:
    #     if sponsor in dbwork.psfullnames:
    #         # print(sponsor)
    #         suindex = dbwork.psfullnames.index(sponsor)
    #         if dbwork.psids[suindex] not in cosponsors_id:
    #             cosponsors_id.append(dbwork.psids[suindex])
    #
    # for sponsor in sponsors:
    #     if sponsor in dbwork.pslastnames:
    #         # print(sponsor)
    #         suindex = dbwork.pslastnames.index(sponsor)
    #         if dbwork.psids[suindex] not in sponsors_id:
    #             sponsors_id.append(dbwork.psids[suindex])
    #
    # for sponsor in sponsors:
    #     if sponsor in dbwork.psfullnames:
    #         # print(sponsor)
    #         suindex = dbwork.psfullnames.index(sponsor)
    #         if dbwork.psids[suindex] not in sponsors_id:
    #             sponsors_id.append(dbwork.psids[suindex])
    #
    # if principal_sponsor in dbwork.pslastnames:
    #     # print(principal_sponsor)
    #     suindex = dbwork.pslastnames.index(principal_sponsor)
    #     principal_sponsor_id = int(dbwork.psids[suindex])
    # elif principal_sponsor in dbwork.psfullnames:
    #     # print(principal_sponsor)
    #     suindex = dbwork.psfullnames.index(principal_sponsor)
    #     principal_sponsor_id = int(dbwork.psids[suindex])
    # else:
    #     principal_sponsor_id = None

    # print(principal_sponsor_id)
    # print(cosponsors_id)
    bill_title = ""
    try:
        short_title = bill_soup.find("label", {"class": "col-md-8"})
        bill_title = short_title.text
    except:
        pass
    current_status = ""
    try:
        final_disposition = bill_soup.find("label", {"class": "col-md-5"})
        current_status = final_disposition.text
    except:
        pass
    date_introduced = None
    try:
        first_read = bill_soup.find("label", {"class": "col-md-9"})
        date = first_read.text
        date_introduced = datetime.datetime.strptime(date, "%m/%d/%Y").strftime("%Y-%m-%d")

    except:
        pass
    votes = []
    actions = []
    committees = []
    events = bill_soup.findAll("div", {"class": "row content-large"})
    chamber = ""
    try:
        for event in events:
            try:
                if "House" in event.div.button.text:
                    chamber = "House"
                elif "Senate" in event.div.button.text:
                    chamber = "Senate"
                else:
                    chamber = "Other"
            except:
                event_info = event.findAll("div", {"class": "col-md-2"})
                event_date = event_info[0].text
                date = datetime.datetime.strptime(event_date, "%m/%d/%Y").strftime("%Y-%m-%d")
                committee = event_info[1].text
                description = event_info[3].text
                action = {'date': date, 'action_by': chamber, 'description': description}
                actions.append(action)
                com = {'chamber': chamber, 'committee': committee}
                committees.append(com)
                vote_data = event.find("div", {"class": "col-md-1"})
                vote_data = vote_data.label.text.split("-")
                if all(v == "0" for v in vote_data):
                    pass
                else:
                    if len(vote_data) == 6:
                        yea = int(vote_data[0])
                        nay = int(vote_data[1])
                        nv = int(vote_data[5])
                        absent = int(vote_data[3])
                        total = yea + nay
                        if yea > nay:
                            passed = 1
                        else:
                            passed = 0
                        vote_info = {'date': date, 'description': description, 'yea': yea, 'nay': nay, 'nv': nv,
                                     'absent': absent,
                                     'total': total, 'passed': passed, 'chamber': chamber, 'votes': []}
                        votes.append(vote_info)
                    elif len(vote_data) == 4:
                        yea = int(vote_data[0])
                        nay = int(vote_data[1])
                        nv = int(vote_data[2])
                        absent = int(vote_data[3])
                        total = yea + nay
                        if yea > nay:
                            passed = 1
                        else:
                            passed = 0
                        vote_info = {'date': date, 'description': description, 'yea': yea, 'nay': nay, 'nv': nv,
                                     'absent': absent,
                                     'total': total, 'passed': passed, 'chamber': chamber, 'votes': []}
                        votes.append(vote_info)
                    else:
                        print(len(vote_info))






    except:
        pass

    tables = bill_soup.findAll("table", {"class": "table table-bordered table-striped table-bsicondensed"})
    for table in tables:
        table_info = table.findAll("td")
        chamber = "Other"
        if table_info[2].text != "0":
            try:

                vote_date = table_info[1].text
                date = datetime.datetime.strptime(vote_date, "%m/%d/%Y").strftime("%Y-%m-%d")
                description = table_info[0].text.replace("Show", "").strip()

                if "house" in description.lower():
                    chamber = "House"
                elif "senate" in description.lower():
                    chamber = "Senate"

                yea = int(table_info[2].text)

                nay = int(table_info[3].text)
                nv = int(table_info[4].text)
                absent = int(table_info[5].text)
                total = nay + yea
                passed_info = table_info[12].text
                if "pass" in passed_info.lower():
                    passed = 1
                else:
                    passed = 0
                vote_info = {'date': date, 'description': description, 'yea': yea, 'nay': nay, 'nv': nv, 'absent': absent,
                             'total': total, 'passed': passed, 'chamber': chamber, 'votes': []}

                votes.append(vote_info)
            except:
                pass

    bill_info = {'source_url': myurl, 'bill_title': bill_title,
                 'current_status': current_status, 'state': 'AZ', 'state_id': 4, 'topic': "",
                 'date_introduced': date_introduced, 'actions': actions, 'committees': committees,
                 'bill_description': "", 'bill_summary': "", 'votes': votes}
    print(bill_info)

    return bill_info


def collect_bill_texts(bill_id):
    bill_text = ""
    try:
        doc_url = 'https://apps.azleg.gov/api/DocType/?billStatusId=' + bill_id
        response = uReq(doc_url).read()
        response = str(response)
        response = response.split("b'")[1]
        response = response.split("MiscBillDocuments")[0]
        response = response.split(",")
        for r in response:
            if "htm" in r:
                pdflink = r.replace('"HtmlPath":"', "")
                pdflink = pdflink.replace('.htm"', '.pdf')

        r = requests.get(pdflink)
        f = io.BytesIO(r.content)
        reader = PyPDF2.PdfFileReader(f, strict=False)
        if reader.isEncrypted:
            reader.decrypt('')

        contents = reader.getPage(0).extractText()
        bill_text = contents
        bill_text = bill_text.replace("\n", "")
        # print(bill_text)
    except:
        # print("issue or no pdf")
        # print(link)
        pass
    topics = []
    try:
        topic_url = 'https://apps.azleg.gov/api/Keyword/?billStatusId=' + bill_id
        response = uReq(topic_url).read()
        response = str(response)
        response = response.split("b'")[1]
        response = response.split("<Name>")

        for r in response:
            # r = r.split("</Name>")[0]
            name_splits = (r.split('"Name":'))
            name = 0
            for ns in name_splits:
                if name == 1:
                    topic = ns.split("}")[0].replace('"', "")
                    topics.append(topic)
                elif name == 0:
                    name = 1

        # print(response)
    except:
        print("faield")
        pass
    # print(topics)
    site_topic = ', '.join(topics)
    # print(site_topic)

    principal_sponsor = ""
    sponsors = []
    cosponsors = []
    principal_sponsor_id = 0
    cosponsors_id = []
    sponsors_id = []
    spon_url = 'https://apps.azleg.gov/api/BillSponsor/?id=' + bill_id
    response = uReq(spon_url).read()
    response = str(response)
    response = response.split("b'")[1]
    response = response.split("},{")
    for r in response:
        r = r.split(",")
        for info in r:
            # print(info)
            if 'SponsorType' in info:
                sponsor_type = info.split('":"')[1]
                sponsor_type = sponsor_type.replace('"', "")
            elif "LastName" in info:
                # print(info)
                sponsor_name = info.split('":"')[1]

                sponsor_name = sponsor_name.replace('"', "").strip()

                sponsor_name = unicodedata.normalize("NFKD", sponsor_name)
                sponsor_name = sponsor_name.split("\\")[0]

            # elif "FirstName" in info:
            #     sponsor_first = info.split('":"')[1]
            #     sponsor_first = sponsor_first.replace('"', "")
        if "Prime" in sponsor_type:
            principal_sponsor = sponsor_name
            # if principal_sponsor in dbwork.pslastnames:
            #     # print(principal_sponsor)
            #     suindex = dbwork.pslastnames.index(principal_sponsor)
            #     principal_sponsor_id = int(dbwork.psids[suindex])
            # elif principal_sponsor in dbwork.psfullnames:
            #     # print(principal_sponsor)
            #     suindex = dbwork.psfullnames.index(principal_sponsor)
            #     principal_sponsor_id = int(dbwork.psids[suindex])
            # else:
            principal_sponsor_id = 0

        elif "Co-Sponsor" in sponsor_type:
            cosponsors.append(sponsor_name)
        #     if sponsor_name in dbwork.pslastnames:
        #         suindex = dbwork.pslastnames.index(sponsor_name)
        #         cosponsors_id.append(int(dbwork.psids[suindex]))
        #     elif sponsor_name in dbwork.psfullnames:
        #         suindex = dbwork.psfullnames.index(sponsor_name)
        #         cosponsors_id.append(int(dbwork.psids[suindex]))
        #
        # else:
        #     sponsors.append(sponsor_name)
        #     if sponsor_name in dbwork.pslastnames:
        #         suindex = dbwork.pslastnames.index(sponsor_name)
        #         sponsors_id.append(int(dbwork.psids[suindex]))
        #     elif sponsor_name in dbwork.psfullnames:
        #         suindex = dbwork.psfullnames.index(sponsor_name)
        #         sponsors_id.append(int(dbwork.psids[suindex]))

    for sponsor in cosponsors:
        search_for = dict(name_last=sponsor)
        try:

            cs_id = scraper_utils.get_legislator_id(**search_for)
            cs_id = int(cs_id)
            # print(cs_id)
            cosponsors_id.append(cs_id)
        except:
            pass
    for sponsor in sponsors:
        search_for = dict(name_last=sponsor)
        try:

            s_id = scraper_utils.get_legislator_id(**search_for)
            s_id = int(s_id)
            sponsors_id.append(s_id)
        except:
            pass
    search_for = dict(name_last=principal_sponsor)
    try:

        ps_id = scraper_utils.get_legislator_id(**search_for)
        principal_sponsor_id = int(ps_id)

    except:
        principal_sponsor_id = 0


    # root = ET.fromstring(str(response).encode("utf-8"))

    # print(root)

    bill_info = {'source_id': bill_id, 'bill_text': bill_text, 'source_topic': site_topic,
                 'principal_sponsor': principal_sponsor, 'principal_sponsor_id': int(principal_sponsor_id),
                 'cosponsors': cosponsors, 'cosponsors_id': cosponsors_id,
                 'sponsors': sponsors, 'sponsors_id': sponsors_id, 'country_id': scraper_utils.country_id,
                 'country': scraper_utils.country}
    # print(bill_info)
    return bill_info


if __name__ == '__main__':
    bills_main = 'https://www.azleg.gov/bills/'

    bill_info = find_bill_links(bills_main)
    links_name_df = pd.DataFrame(bill_info)
    # print(links_name_df)
    links = links_name_df["source_url"]
    bill_ids = links_name_df['source_id']
    # lesslinks = links[:50]
    # lessids = bill_ids[:50]

    #
    with Pool() as pool:
        bill_data = pool.map(func=collect_bill_data, iterable=links)
    bill_df = pd.DataFrame(bill_data)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_columns', None)

    bill_df = pd.merge(bill_df, links_name_df, how='left', on=['source_url'])
    # print(bill_df)

    with Pool() as pool:
        bill_text_data = pool.map(func=collect_bill_texts, iterable=bill_ids)
    bill_text_df = pd.DataFrame(bill_text_data)
    # print(bill_text_df)

    bill_df = pd.merge(bill_df, bill_text_df, how='left', on=['source_id'])
    bill_df['principal_sponsor_id'] = bill_df['principal_sponsor_id'].replace({np.nan: None})
    bill_df['principal_sponsor_id'] = [int(d) if d else d for d in bill_df['principal_sponsor_id']]
    #
    # big_df = topics.add_topics(bill_df)
    print(bill_df)

    big_list_of_dicts = bill_df.to_dict('records')
    # print(*big_list_of_dicts, sep="\n")

    print('Writing data to database...')
    scraper_utils.insert_legislation_data_into_db(big_list_of_dicts)

    print('Complete!')