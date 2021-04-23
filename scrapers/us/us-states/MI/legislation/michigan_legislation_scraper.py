from legislation_scraper_utils import USStateLegislationScraperUtils
import io
import requests
import PyPDF2
from string import ascii_uppercase
import re
import datetime
from multiprocessing import Pool
import unidecode
import datefinder
from nameparser import HumanName
import psycopg2
from bs4 import BeautifulSoup as soup
from urllib.request import Request
from urllib.request import urlopen as uReq
import pandas as pd
import utils
import unicodedata
import time
import argparse
import gzip
import numpy as np
import pickle
import os
import json
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))


state_abbreviation = 'MI'
database_table_name = 'us_mi_legislation'
legislator_table_name = 'us_mi_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)


def get_bill_info(link):
    bill_text = ""
    principal_sponsor = ""
    principal_sponsor_id = 0
    sponsors_id = []
    current_status = ""
    date_introduced = None
    bill_name = ""

    try:
        uClient = uReq(link)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")
        state_url = link
        if "HR" in link:
            chamber_origin = "House"
            bill_type = "Resolution"
            second_half = link.split("HR-")[1]
            bill_num = second_half.split("&")[0]
            bill_name = "HR" + bill_num

        elif "SR" in link:
            chamber_origin = "Senate"
            bill_type = "Resolution"
            second_half = link.split("SR-")[1]
            bill_num = second_half.split("&")[0]
            bill_name = "SR" + bill_num

        elif "SB" in link:
            chamber_origin = "Senate"
            bill_type = "Bill"
            second_half = link.split("SB-")[1]
            bill_num = second_half.split("&")[0]
            bill_name = "SB" + bill_num

        elif "HB" in link:
            chamber_origin = "House"
            bill_type = "Bill"
            second_half = link.split("HB-")[1]
            bill_num = second_half.split("&")[0]
            bill_name = "HB" + bill_num
        elif "HJR" in link:
            chamber_origin = "House"
            bill_type = "Joint Resolution"
            second_half = link.split("HJR-")[1]
            bill_num = second_half.split("&")[0]
            bill_name = "HJR" + bill_num
        elif "SJR" in link:
            chamber_origin = "Senate"
            bill_type = "Joint Resolution"
            second_half = link.split("SJR-")[1]
            bill_num = second_half.split("&")[0]
            bill_name = "SJR" + bill_num

        sponsors = []
        sponurls = []
        sponsors_id = []
        sponsorList = page_soup.find(
            "span", {"id": "frg_billstatus_SponsorList"})
        sl = sponsorList.findAll("a", {"class": "personPopupTrigger"})
        if len(sl) > 0:
            for s in sl:

                sponsor = s.text.replace("\xa0", " ")
                sponsor_full = HumanName(sponsor)

                sponsors.append(sponsor_full.last)
                sponurl = s["href"]

                search_for = dict(name_last=sponsor_full.last,
                                  source_url=sponurl)
                try:

                    s_id = scraper_utils.get_legislator_id(**search_for)
                    s_id = int(s_id)

                    sponsors_id.append(s_id)
                except:
                    pass

            # for sponsor in sponsors:
            #     if sponsor in dbwork.psfullnames:
            #         suindex = dbwork.psfullnames.index(sponsor)
            #         if dbwork.psids[suindex] not in sponsors_id:
            #             sponsors_id.append(dbwork.psids[suindex])
            #
            # for su in sponurls:
            #     if su in dbwork.psurls:
            #         suindex = dbwork.psurls.index(su)
            #         if dbwork.psids[suindex] not in sponsors_id:
            #             sponsors_id.append(dbwork.psids[suindex])

        if len(sponsors) > 0:
            principal_sponsor = sponsors[0]
        if len(sponsors_id) > 0:
            principal_sponsor_id = sponsors_id[0]
        committees = []
        topics = []
        categorylist = page_soup.find(
            "span", {"id": "frg_billstatus_CategoryList"})
        if categorylist:
            # categories section exists
            categories = categorylist.findAll("a")

            for c in categories:
                if "committee" in c.text.lower():
                    comm = {'chamber': chamber_origin, 'committee': c.text}
                    committees.append(comm)
                else:
                    topics.append(c.text)

        site_topic = ",".join(topics)

        words_sections = page_soup.find(
            "span", {"id": "frg_billstatus_ObjectSubject"})
        if words_sections:
            bill_summary = words_sections.text

        actions = []
        votes = []
        history_table = page_soup.find(
            "table", {"id": "frg_billstatus_HistoriesGridView"})
        events = history_table.findAll("tr")
        events = events[0:]
        for event in events:
            event_info = event.findAll("td")
            i = 1
            for ei in event_info:
                if i % 3 == 1:
                    date = ei.text
                    # date = datetime.datetime.strptime(date, "%m/%d/%Y").strftime("%Y-%m-%d")

                if i % 3 == 2:
                    journal = ei.text
                    if "HJ" in journal:
                        action_by = "House"
                    elif "SJ" in journal:
                        action_by = "Senate"
                    else:
                        action_by = chamber_origin

                if i % 3 == 0:
                    description = ei.text
                    if "roll call" in description.lower():
                        # print(description)
                        # these are votes
                        try:

                            split_on_yeas = description.lower().split("yeas ")[
                                1]
                            split_on_nays = description.lower().split("nays ")[
                                1]
                            split_on_excused = description.lower().split("excused ")[
                                1]

                            yea = split_on_yeas.split(" nays")[0]
                            nay = split_on_nays.split(" excused")[0]
                            absent = split_on_excused.split(" not voting")[0]
                            nv = description.lower().split("not voting ")[1]
                            total = int(yea) + int(nay)
                            chamber = action_by
                            if "passed" in description.lower():
                                passed = 1
                            else:
                                passed = 0
                            vote_event_info = {'date': date, 'description': "", 'yea': yea, 'nay': nay, 'absent': absent,
                                               'total': total, 'passed': passed, 'chamber': chamber, 'votes': []}
                            votes.append(vote_event_info)
                            # print(vote_event_info)
                        except:
                            pass

                    action = {'date': date, 'action_by': action_by,
                              'description': description}

                    actions.append(action)

                i = i + 1
        try:
            first_action = actions[0]
            date_introduced = first_action["date"]
        except:
            pass

        actions.reverse()

        # get current status by most recent action

        recent_action = actions[0]

        current_status = recent_action["description"]

        # get bill_text from pdf
        status_table = page_soup.find(
            "table", {"id": "frg_billstatus_DocumentGridTable"})

        bill_text = ""
        doclist = status_table.findAll("a", {"target": "_top"})
        for doc in doclist:
            doc_link = "https://www.legislature.mi.gov" + \
                (doc["href"]).replace("..", "")
            if ".pdf" in doc_link:
                pdf_link = doc_link
        try:
            r = requests.get(pdf_link)
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
            print(bill_text)
        except:
            # print("issue or no pdf")
            # print(link)
            pass

    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        print(link)

    # get goverlytics_id, url
    goverlytics_id = "MI_2019-2020_" + bill_name
    url = "/us/MI/legislation/" + goverlytics_id

    bill_info = {'goverlytics_id': goverlytics_id, 'url': url, 'state': 'MI', 'state_id': 26, 'state_url': state_url,
                 'bill_type': bill_type, 'chamber_origin': chamber_origin, 'bill_name': bill_name, 'sponsors': sponsors,
                 'sponsors_id': sponsors_id, 'principal_sponsor': principal_sponsor,
                 'principal_sponsor_id': int(principal_sponsor_id), 'cosponsors': [], 'cosponsors_id': [],
                 'committees': committees, 'site_topic': site_topic, 'session': '2020-2021',
                 'bill_summary': bill_summary, 'topic': "", 'bill_state_id': "", 'actions': actions,
                 'current_status': current_status, 'date_introduced': date_introduced, 'bill_text': bill_text,
                 'bill_description': "", 'bill_state_id': "", 'bill_title': "", 'votes': votes}
    # print(bill_info)
    return bill_info


if __name__ == '__main__':

    bill_infos = []

    failed = 0
    i = 1
    while failed == 0:
        leadingZeros = (4 - len(str(i))) * "0"
        stringi = leadingZeros + str(i)

        bill_link = 'https://www.legislature.mi.gov/(S(mpcg5ujzjedicorlgbeczyf0))/mileg.aspx?page=getobject&objectname=2021-SB-'\
                    + stringi + '&query=on'
        print(bill_link)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1

    failed = 0
    i = 4001
    while failed == 0:

        bill_link = 'https://www.legislature.mi.gov/(S(mpcg5ujzjedicorlgbeczyf0))/mileg.aspx?page=getobject&objectname=2021-HB-'\
                    + str(i) + '&query=on'
        print(bill_link)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1
    #
    failed = 0
    i = 1
    while failed == 0:
        leadingZeros = (4 - len(str(i))) * "0"
        stringi = leadingZeros + str(i)

        bill_link = 'https://www.legislature.mi.gov/(S(mpcg5ujzjedicorlgbeczyf0))/mileg.aspx?page=getobject&objectname=2021-HR-' \
                    + stringi + '&query=on'
        print(bill_link)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1

    failed = 0
    i = 1
    while failed == 0:
        leadingZeros = (4 - len(str(i))) * "0"
        stringi = leadingZeros + str(i)

        bill_link = 'https://www.legislature.mi.gov/(S(mpcg5ujzjedicorlgbeczyf0))/mileg.aspx?page=getobject&objectname=2021-SR-' \
                    + stringi + '&query=on'
        print(bill_link)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1

    failed = 0
    i = 0
    while failed == 0:

        stringi = ascii_uppercase[i]
        print(stringi)

        bill_link = 'https://www.legislature.mi.gov/(S(mpcg5ujzjedicorlgbeczyf0))/mileg.aspx?page=getobject&objectname=2021-SJR-' \
                    + stringi + '&query=on'
        print(bill_link)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1

    failed = 0
    i = 0
    while failed == 0:

        stringi = ascii_uppercase[i]
        print(stringi)

        bill_link = 'https://www.legislature.mi.gov/(S(mpcg5ujzjedicorlgbeczyf0))/mileg.aspx?page=getobject&objectname=2021-HJR-' \
                    + stringi + '&query=on'
        print(bill_link)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1

    big_df = pd.DataFrame(bill_infos)

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    # big_df = topics.add_topics(big_df)
    big_df['source_url'] = big_df['state_url']
    big_df['source_id'] = big_df['bill_state_id']
    big_df['source_topic'] = big_df['site_topic']

    sample_row = scraper_utils.initialize_row()
    # print(sample_row)
    #

    big_df['state'] = sample_row.state
    big_df['state_id'] = sample_row.state_id

    big_df['country'] = sample_row.country
    # # #
    big_df['country_id'] = sample_row.country_id

    print(big_df)

    big_list_of_dicts = big_df.to_dict('records')
    # print(big_list_of_dicts)

    print('Writing data to database...')
    scraper_utils.insert_legislation_data_into_db(big_list_of_dicts)

    print('Complete!')
