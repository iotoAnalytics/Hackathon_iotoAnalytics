import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))
import io
from legislation_scraper_utils import CadProvinceTerrLegislationScraperUtils
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

prov_terr_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))

database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
legislator_table_name = str(configParser.get('scraperConfig', 'legislator_table_name'))

scraper_utils = CadProvinceTerrLegislationScraperUtils(prov_terr_abbreviation,
                                                       database_table_name,
                                                       legislator_table_name)


def scrape_bill_link(myurl):
    sponsors = []
    bill_name = myurl.split("projet-")[1]
    bill_split_42 = bill_name.split("-42")
    if len(bill_split_42) == 2:
        bill_name = bill_split_42[len(bill_split_42) - 2]
    else:
        bill_name = bill_split_42[0] + '-42'

    # get goverlytics_id, url
    goverlytics_id = "QC_2018_" + bill_name
    url = "/cad/QC/legislation/" + goverlytics_id

    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    title = page_soup.find("div", {"class": "colonneImbriquee imbGauche"})
    title = title.h1.span.span.text
    title = title.split(", ")[1:]
    bill_description = ', '.join(title)

    sponsor_column = page_soup.find("td", {"class": "colonneDate"})
    principal_sponsor = sponsor_column.a.text
    prinspon_first = principal_sponsor.split(",")[1].strip()
    prinspon_last = principal_sponsor.split(",")[0]
    principal_sponsor = prinspon_first + " " + prinspon_last
    sponsors.append(principal_sponsor)
    principal_sponsor_id = 0
    # if principal_sponsor in dbwork.psfullnames:
    #     suindex = dbwork.psfullnames.index(principal_sponsor)
    #
    #     principal_sponsor_id = dbwork.psids[suindex]

    bill_text = ""
    try:

        list_pdf = page_soup.find("ul", {"class": "ListeLien"})
        pdf_link = list_pdf.li.a["href"]

        r = requests.get(pdf_link)
        f = io.BytesIO(r.content)
        reader = PyPDF2.PdfFileReader(f, strict=False)
        if reader.isEncrypted:
            reader.decrypt('')

        contents = reader.getPage(0).extractText()
        bill_text = contents
        bill_text = bill_text.replace("\n", "")
        # print(bill_text)
    except:
        print("issue or no pdf")
        print(pdf_link)
        pass
    bill_id = myurl.split("loi-")[1]
    bill_id = bill_id.split(".html")[0]

    actions = []
    committees = []
    possible_actions = []
    try:
        event_lists = page_soup.findAll("ul", {"class": "ListeLien"})
        for el in event_lists:
            lis = el.findAll("li")
            for li in lis:
                possible_action = (li.text)
                possible_action = " ".join(possible_action.split())
                possible_actions.append(possible_action)
    except:
        pass
    votes = []
    months_check = ["Jan", "Feb", "March", "Apr", "May", "June", "July", "Aug", "Sept", "Oct", "Nov", "Dec"]
    date_introduced = None
    for pa in possible_actions:
        if any(ext in pa for ext in months_check):
            if "(" in pa:
                event_description = pa.split("(")[1].replace(")", "")
            else:
                event_description = pa

            date_piece = pa.split("(")[0]
            date = None
            try:
                date_piece = date_piece.split("on")[1].strip()

                date = datetime.datetime.strptime(date_piece, "%B %d, %Y")
                date = date.strftime('%Y-%m-%d')

                if "vote : " in event_description.lower():
                    # print(event_description)
                    pour = event_description.lower().split("pour")[1].strip()
                    pour = pour.split(",")[0].strip()
                    pour = pour.replace(":", "").strip()
                    yea = int(pour)
                    contre = event_description.lower().split("contre")[1].strip()
                    contre = contre.split(",")[0].strip()
                    contre = contre.replace(":", "").strip()
                    nay = int(contre)
                    abstention = event_description.lower().split("abstention")[1]
                    abstention = abstention.replace("s", "").strip()
                    abstention = abstention.split(";")[0].strip()
                    abstention = abstention.replace(":", "").strip()
                    nv = int(abstention)
                    absent = 0
                    total = yea + nay
                    if yea > nay:
                        passed = 1
                    else:
                        passed = 0
                    vote_info = {'date': date, 'description': event_description, 'yea': yea, 'nay': nay, 'nv': nv,
                                 'absent': absent, 'total': total, 'passed': passed, 'chamber': 'National Assembly',
                                 'votes': []}
                    votes.append(vote_info)


            except Exception as ex:

                template = "An exception of type {0} occurred. Arguments:\n{1!r}"

                message = template.format(type(ex).__name__, ex.args)

                # print(message)
                # print(event_description)
            if "pdf" not in event_description:
                action = {'date': date, 'action_by':'National Assembly', 'description': event_description}
                # print(action)
                if action not in actions:
                    actions.append(action)


        if "Committee" in pa:
            com_name = "Committee" + pa.split("Committee")[1]
            com_name = com_name.split("-")[0].strip()
            com_name = com_name.split(",")[0].strip()
            com_name = com_name.split("(")[0].strip()
            # print(com_name)
            committee_info = {'chamber': 'National Assembly', 'committee': com_name}
            if committee_info not in committees:
                committees.append(committee_info)
    try:
        actions = sorted(actions, key=lambda action: action['date'])
        votes = sorted(votes, key=lambda vote: vote['date'])

    except:
        pass

    try:
        date_introduced = actions[0]["date"]
        # print(date_introduced)


    except:
        pass
    actions.reverse()
    votes.reverse()

    current_status = actions[0]["description"]



    # print(votes)
    info = {'source_url': myurl, 'bill_name': bill_name, 'chamber_origin': 'National Assembly',
            'bill_type': 'Bill',
            'province_territory': 'QC', 'province_territory_id': 24, 'session': 2018, 'goverlytics_id': goverlytics_id,
            'url': url, 'bill_description': bill_description, 'principal_sponsor': principal_sponsor,
            'principal_sponsor_id': principal_sponsor_id, 'sponsors': sponsors, 'sponsors_id': [], 'cosponsors': [],
            'cosponsors_id': [], 'bill_text': bill_text, 'source_id': bill_id, 'bill_summary':"", 'bill_title': "",
            'committees': committees, 'source_topic': "", 'topic': "", 'actions': actions, 'votes': votes,
            'date_introduced': date_introduced, 'current_status': current_status}
    # print(info)
    return info


if __name__ == '__main__':
    bill_infos = []
    # failed = 0
    # i = 1
    # while failed == 0:
    #     bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
    #     try:
    #         bill_info = scrape_bill_link(bill_link)
    #         if bill_info not in bill_infos:
    #             bill_infos.append(bill_info)
    #     except:
    #         failed = 1
    #     i += 1
    #
    # failed = 0
    # i = 82
    # while failed == 0:
    #     bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
    #     try:
    #         bill_info = scrape_bill_link(bill_link)
    #         if bill_info not in bill_infos:
    #             bill_infos.append(bill_info)
    #     except:
    #         failed = 1
    #     i += 1
    #
    # failed = 0
    # i = 190
    # while failed == 0:
    #     bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
    #     try:
    #         bill_info = scrape_bill_link(bill_link)
    #         if bill_info not in bill_infos:
    #             bill_infos.append(bill_info)
    #     except:
    #         failed = 1
    #     i += 1
    #
    # failed = 0
    # i = 390
    # while failed == 0:
    #     bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
    #     try:
    #         bill_info = scrape_bill_link(bill_link)
    #         if bill_info not in bill_infos:
    #             bill_infos.append(bill_info)
    #     except:
    #         failed = 1
    #     i += 1
    #
    # failed = 0
    # i = 396
    # while failed == 0:
    #     bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
    #     try:
    #         bill_info = scrape_bill_link(bill_link)
    #         if bill_info not in bill_infos:
    #             bill_infos.append(bill_info)
    #     except:
    #         failed = 1
    #     i += 1
    #
    # failed = 0
    # i = 490
    # while failed == 0:
    #     bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
    #     try:
    #         bill_info = scrape_bill_link(bill_link)
    #         if bill_info not in bill_infos:
    #             bill_infos.append(bill_info)
    #     except:
    #         failed = 1
    #     i += 1
    #
    # failed = 0
    # i = 495
    # while failed == 0:
    #     bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
    #     try:
    #         bill_info = scrape_bill_link(bill_link)
    #         if bill_info not in bill_infos:
    #             bill_infos.append(bill_info)
    #     except:
    #         failed = 1
    #     i += 1
    #
    # #
    # failed = 0
    # i = 590
    # while failed == 0:
    #     bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
    #     try:
    #         bill_info = scrape_bill_link(bill_link)
    #         if bill_info not in bill_infos:
    #             bill_infos.append(bill_info)
    #     except:
    #         failed = 1
    #     i += 1
    #
    # failed = 0
    # i = 594
    # while failed == 0:
    #     bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
    #     try:
    #         bill_info = scrape_bill_link(bill_link)
    #         if bill_info not in bill_infos:
    #             bill_infos.append(bill_info)
    #     except:
    #         failed = 1
    #     i += 1
    # #
    # failed = 0
    # i = 690
    # while failed == 0:
    #     bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
    #     try:
    #         bill_info = scrape_bill_link(bill_link)
    #         if bill_info not in bill_infos:
    #             bill_infos.append(bill_info)
    #     except:
    #         failed = 1
    #     i += 1
    #
    # failed = 0
    # i = 695
    # while failed == 0:
    #     bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
    #     try:
    #         bill_info = scrape_bill_link(bill_link)
    #         if bill_info not in bill_infos:
    #             bill_infos.append(bill_info)
    #     except:
    #         failed = 1
    #     i += 1

    failed = 0
    i = 698
    while failed == 0:
        bill_link = 'http://www.assnat.qc.ca/en/travaux-parlementaires/projets-loi/projet-loi-' + str(i) + '-42-1.html'
        try:
            bill_info = scrape_bill_link(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
        except:
            failed = 1
        i += 1

    bill_info_df = pd.DataFrame(bill_infos)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    print(bill_info_df)

    big_list_of_dicts = bill_info_df.to_dict('records')
    # print(*big_list_of_dicts, sep="\n")

    print('Writing data to database...')
    scraper_utils.insert_legislation_data_into_db(big_list_of_dicts)
    print("Complete!")
