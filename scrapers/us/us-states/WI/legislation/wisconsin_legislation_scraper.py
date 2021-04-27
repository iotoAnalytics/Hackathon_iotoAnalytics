'''
This website will block your IP if too many requests are sent

The data for 2021 session hasn't fully been added to the database because I got blocked from their website..
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

import numpy as np
from multiprocessing import Pool
import pandas as pd
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import urllib.parse as urlparse
from urllib.parse import parse_qs
import datetime
import boto3
from urllib.request import urlopen as uReq
from urllib.request import Request
from bs4 import BeautifulSoup as soup
import psycopg2
from scraper_utils import USStateLegislationScraperUtils
import datefinder
import unidecode
import PyPDF2
import requests
import io

# from selenium import webdriver


# # Initialize config parser and get variables from config file
# configParser = configparser.RawConfigParser()
# configParser.read('config.cfg')

state_abbreviation = 'WI'
database_table_name = 'us_wi_legislation'
legislator_table_name = 'us_wi_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)
crawl_delay = scraper_utils.get_crawl_delay(
    'https://docs.legis.wisconsin.gov/')


def get_assembly_bills(myurl):
    # header = {'User-Agent': 'Mozilla/5.0'}
    # link_request = make_request(myurl, header)
    #
    #
    # print(link_request)
    bill_links = []
    req = Request(myurl,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()

    uReq(req).close()
    scraper_utils.crawl_delay(crawl_delay)

    page_soup = soup(webpage, "html.parser")
    li_odd = page_soup.findAll("li", {"class": "odd"})
    li_even = page_soup.findAll("li", {"class": "even"})
    for li in li_odd:
        link = 'https://docs.legis.wisconsin.gov/' + (li.div.div.a["href"])

        bill_links.append(link)
    for li in li_even:
        link = 'https://docs.legis.wisconsin.gov/' + (li.div.div.a["href"])
        bill_links.append(link)

    return bill_links


# def get_ammendment_links(myurl):
#     bill_links = []
#     req = Request(myurl,
#                   headers={'User-Agent': 'Mozilla/5.0'})
#     webpage = uReq(req).read()
#
#     uReq(req).close()
#
#     page_soup = soup(webpage, "html.parser")
#     doclinks = page_soup.find("ul", {"class": "docLinks"})
#     doclinks = doclinks.findAll("li")
#     for dl in doclinks:
#         url = dl.p.a["href"]
#         print(url)
#         link = 'https://docs.legis.wisconsin.gov/' + url
#         print(link)

def collect_bill_data(myurl):
    uClient = uReq(myurl)
    scraper_utils.crawl_delay(crawl_delay)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    if "sen" in myurl:
        chamber_origin = "Senate"

    elif "asm" in myurl:
        chamber_origin = "House"

    if "bill" in myurl:
        bill_type = "Bill"
        bill_name = myurl.split("bill/")[1]

    elif "joint_resolution" in myurl:
        bill_type = "Joint Resolution"
        bill_name = myurl.split("resolution/")[1]
    elif "resolution" in myurl:
        bill_type = "Resolution"
        bill_name = myurl.split("resolution/")[1]

    status_class = page_soup.find("div", {"class": "propStatus"})
    current_status = (status_class.h2.text.split(":")[1].strip())

    actions = []
    committees = []
    cosponsors = []
    votes = []
    history_table = page_soup.find("div", {"class": "propHistory"})
    history_events = history_table.findAll("tr")[1:]
    for event in history_events:
        event_info = event.findAll("td")
        date_house = (event_info[0].text)
        date = date_house.split(" ")[0]
        date = datetime.datetime.strptime(
            date, "%m/%d/%Y").strftime("%Y-%m-%d")
        house_abbrev = date_house.split(" ")[1]
        if house_abbrev == "Asm.":
            action_by = "Assembly"
        elif house_abbrev == "Sen.":
            action_by = "Senate"
        else:
            action_by = ""
        description = event_info[1].text
        try:
            if "committee" in event_info[1].a.text:
                com_name = event_info[1].a.text
                com_chamber = action_by
                com_info = {'chamber': com_chamber, 'committee': com_name}
                committees.append(com_info)
        except:
            pass
        if "cosponsor" in event_info[1].text.lower():
            cosponsors_string = (event_info[1].text.split("osponsor")[1])
            if "epresentatives" in cosponsors_string:
                cosponsors_string = cosponsors_string.split("epresentatives")[
                    1].strip()
                cosponsors_string = cosponsors_string.replace(" and ", ";")
                cos = re.split(', |;', cosponsors_string)
                for co in cos:
                    if co not in cosponsors:
                        cosponsors.append(co)

            elif "enators" in cosponsors_string:
                cosponsors_string = cosponsors_string.split("enators")[
                    1].strip()
                cosponsors_string = cosponsors_string.replace(" and ", ";")
                cos = re.split(', |;', cosponsors_string)
                for co in cos:
                    if co not in cosponsors:
                        cosponsors.append(co)
        if "added as a cosponsor" in event_info[1].text:
            cosponsors_string = (event_info[1].text.split("added")[0])
            cosp = cosponsors_string.strip()
            cosp = cosp.replace("Senator", "")
            cosp = cosp.replace("Represenative", "")
            cosp = cosp.strip()
            # print(cosp)
            if cosp not in cosponsors:
                cosponsors.append(cosp)
        if "Ayes" in event_info[1].text:
            vote_numbers = event_info[1].text.split("Ayes ")[1]
            yea = vote_numbers.split(",")[0].strip()
            yea = int(yea)
            nay = vote_numbers.split("Noes")[1].strip()
            try:
                nay = int(nay)
            except:
                nay = 0
            total = yea + nay
            vote_description = event_info[1].text.split(", Ayes ")[0].strip()
            nv = 0
            absent = 0
            if yea > nay:
                passed = 1
            else:
                passed = 0

            vote_info = {'date': date, 'description': vote_description, 'yea': yea, 'nay': nay, 'nv': nv,
                         'absent': absent, 'total': total, 'passed': passed, 'chamber': action_by, 'votes': []}
            votes.append(vote_info)

        # print(votes)

        action = {'date': date, 'action_by': action_by,
                  'description': description}

        actions.append(action)
    date_introduced = actions[0]["date"]

    actions.reverse()

    box_content = page_soup.find("div", {"class": "box-content"})
    box_p = box_content.find("p").text

    bill_description = box_p
    votes.reverse()

    haspdflink = page_soup.find("span", {"class": "hasPdfLink"})
    pdf_link = "https://docs.legis.wisconsin.gov" + (haspdflink.span.a["href"])

    try:
        r = scraper_utils.request(pdf_link)
        scraper_utils.crawl_delay(crawl_delay)
        f = io.BytesIO(r.content)
        reader = PyPDF2.PdfFileReader(f, strict=False)
        if reader.isEncrypted:
            reader.decrypt('')

        contents = reader.getPage(0).extractText()
        bill_text = contents
        bill_text = bill_text.replace("\n", "")

    except:
        # print("issue or no pdf")
        # print(link)
        pass

    session_url = myurl.split("gov//")[1]
    session = session_url.split("/proposals")[0]

    goverlytics_id = "WI_2021_" + bill_name
    url = "/us/WI/legislation/" + goverlytics_id
    cosponsors_id = []
    for cosponsor in cosponsors:
        search_for = dict(name_last=cosponsor)
        try:
            sponsor_id = scraper_utils.get_legislator_id(**search_for)
            cosponsors_id.append(int(sponsor_id))
        except:
            pass
    # print(cosponsors)
    # print(cosponsors_id)

    info = {'state_url': myurl, 'chamber_origin': chamber_origin, 'bill_type': bill_type, 'state': 'WI', 'state_id': 55,
            'bill_name': bill_name, 'current_status': current_status, 'actions': actions,
            'bill_description': bill_description, 'committees': committees, 'cosponsors': cosponsors,
            'cosponsors_id': cosponsors_id,
            'bill_text': bill_text, 'bill_state_id': "", 'session': session, 'goverlytics_id': goverlytics_id,
            'url': url, 'bill_title': bill_name, 'bill_summary': "", 'source_topic': "", 'bill_state_id': bill_name,
            'date_introduced': date_introduced, 'votes': votes, 'topic': "", 'principal_sponsor': "",
            'principal_sponsor_id': 0, 'sponsors': [], 'sponsors_id': [], 'country_id': scraper_utils.country_id,
            'country': scraper_utils.country}
    print(info)
    return info


if __name__ == '__main__':

    # alink = 'https://docs.legis.wisconsin.gov/2021/amendments'
    ablink = 'https://docs.legis.wisconsin.gov/2021/proposals/reg/asm/bill'
    ajrlink = 'https://docs.legis.wisconsin.gov/2021/proposals/reg/asm/joint_resolution'
    arlink = 'https://docs.legis.wisconsin.gov/2021/proposals/reg/asm/resolution'
    sblink = 'https://docs.legis.wisconsin.gov/2021/proposals/reg/sen/bill'
    sjrlink = 'https://docs.legis.wisconsin.gov/2021/proposals/reg/sen/joint_resolution'
    srlink = 'https://docs.legis.wisconsin.gov/2021/proposals/reg/sen/resolution'

    # abs = get_assembly_bills(ablink)
    ajrs = get_assembly_bills(ajrlink)
    # ars = get_assembly_bills(arlink)
    # #
    # sbs = get_assembly_bills(sblink)
    # sjrs = get_assembly_bills(sjrlink)
    # srs = get_assembly_bills(srlink)

    # amm = get_ammendment_links(alink)

    all_links = []
    # for a in abs:
    #     all_links.append(a)
    for aj in ajrs:
        all_links.append(aj)
    # for ar in ars:
    #     all_links.append(ar)
    # for sb in sbs:
    #     all_links.append(sb)

    # for sjr in sjrs:
    #     all_links.append(sjr)
    # for sr in srs:
    #     all_links.append(sr)
    print(len(all_links))
    # less_links = all_links
    # less_links = less_links[:5]
    # print(less_links)

    with Pool() as pool:
        bill_data = pool.map(func=collect_bill_data, iterable=all_links)
    bill_df = pd.DataFrame(bill_data)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    bill_df['source_id'] = bill_df['bill_state_id']
    bill_df['source_url'] = bill_df['state_url']
    print(bill_df)

    # print(big_df)
    big_list_of_dicts = bill_df.to_dict('records')
    # print(big_list_of_dicts)

    print('Writing data to database...')
    scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
