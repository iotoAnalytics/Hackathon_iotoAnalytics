import os

import numpy as np
import pandas as pd
from zipfile import ZipFile
from io import StringIO, BytesIO
import urllib.request as urllib2
import requests
import sys
import os
from pathlib import Path
import time
from selenium import webdriver
import scraper_utils
from scraper_utils import FinancialContributionsScraperUtils
from database import CursorFromConnectionFromPool
from bs4 import BeautifulSoup
import pandas as pd
import dateutil.parser as dparser

NODES_TO_ROOT = 4
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

# https://www.elections.ca/content.aspx?section=fin&dir=oda&document=index&lang=e
COUNTRY = 'ca'
TABLE = 'ca_financial_contributions'
MAIN_URL = 'https://www.elections.ca'
CONTRIBUTIONS_URL = MAIN_URL + '/content.aspx?section=fin&dir=oda&document=index&lang=e'
THREADS_FOR_POOL = 12
#
scraper_utils = FinancialContributionsScraperUtils(COUNTRY, TABLE)
crawl_delay = scraper_utils.get_crawl_delay(MAIN_URL)

with CursorFromConnectionFromPool() as cur:
    try:
        query = 'SELECT * FROM ca_candidates'
        cur.execute(query)
        candidates = cur.fetchall()

        query = 'SELECT * FROM ca_electoral_districts'
        cur.execute(query)
        electoral_districts = cur.fetchall()

    except Exception as e:
        sys.exit(
            f'An exception occurred retrieving tables from database:\n{e}')

    candidates_df = pd.DataFrame(candidates)
    districts = pd.DataFrame(electoral_districts)

def get_urls():
    urls = []

    page = scraper_utils.request(CONTRIBUTIONS_URL)
    soup = BeautifulSoup(page.content, 'html.parser')

    links = soup.find_all('a')
    for l in links:
        if "Download contributions" in l.text:
            if "submitted" not in l.text:
                urls.append(MAIN_URL + l.get('href'))
    return urls


def read_csv(url):
    print(url)
    #r = urllib2.urlopen("https://www.elections.ca/fin/oda/od_cntrbtn_audt_e.zip").read()
    r = requests.get(url)
    file = ZipFile(BytesIO(r.content))
    if "od_cntrbtn_audt_e" in url:
        contributions_csv = file.open("PoliticalFinance/od_cntrbtn_audt_e.csv")
    else:
        csv_file = url.split('/')[-1]
        csv_file = csv_file.replace('.zip', '.csv')
        contributions_csv = file.open(csv_file)
    try:
        chunk = pd.read_csv(contributions_csv, chunksize=1000000, iterator=True, engine='python', error_bad_lines=False, encoding='latin-1',dtype={"Political Entity": str,
                                                     "Recipient ID": str,
                                                     "Recipient": str,
                                                     "Recipient last name": str,
                                                     "Recipient first name": str,
                                                     "Recipient middle initial": str,
                                                     "Political Party of Recipient": str,
                                                     "Electoral District": str,
                                                     "Electoral event": str,
                                                     "Fiscal/Election date": str,
                                                     "Form ID": str,
                                                     "Financial Report": str,
                                                     "Part Number of Return": str,
                                                     "Financial Report part": str,
                                                     "Contributor type": str,
                                                     "Contributor name": str,
                                                     "Contributor last name": str,
                                                     "Contributor first name": str,
                                                     "Contributor middle initial": str,
                                                     "Contributor City": str,
                                                     "Contributor Province": str,
                                                     "Contributor Postal code": str,
                                                     "Contribution Received date": str,
                                                     "Monetary amount": float,
                                                     "Non-Monetary amount": float,
                                                     "Contribution given through": str
                                                    })
    except Exception as e:
        print(e)

    pd_df = pd.concat(chunk)
    pd_df = pd_df.fillna(0)
    list_of_dicts = pd_df.to_dict('records')
    print(list_of_dicts[1])

    new_list = format_dicts(list_of_dicts)
    new_list = remove_bad_values(new_list)
    return new_list


def remove_bad_values(new_list):
    list = [i for i in new_list if not (i['recipient_id'] == 0)]
    list = [i for i in list if not (i['recipient_party_id'] == 0)]
    list = [i for i in list if not (i['contributor_prov_terr_id'] == 0)]
    return list


def format_dicts(list_of_dicts):
    new_list_of_dicts = []
    for i in list_of_dicts:
        #print(i)
        if "Political Party of Recipient" in i.keys():
            party_id = get_party_id(i['Political Party of Recipient'])
        else:
            party_id = get_party_id(i['Political Party'])
        if 'Contributor Province' in i.keys():
            cont_prov_id = get_prov_terr_id(i['Contributor Province'], i['Electoral District'])
        else:
            cont_prov_id = 0
        if ' Contributor name' in i.keys():
            cont_name = i[' Contributor name']
        else:
            cont_name = i['Contributor name']
        if 'Contributor City' in i.keys():
            city = i['Contributor City']
        else:
            city = None
        if 'Contributor Postal code' in i.keys():
            post_code = i['Contributor Postal code']
        else:
            post_code = None
        if 'Part Number of Return' in i.keys():
            part_no = i['Part Number of Return']
        else:
            part_no = None
        if 'Monetary amount' in i.keys():
            money = i['Monetary amount']
        else:
            money = i['Contribution amount']
        if 'Monetary amount' in i.keys():
            non_money = i['Non-Monetary amount']
        else:
            non_money = 0
        if 'Fiscal date' in i.keys():
            fiscal_date = i['Fiscal date']
            if fiscal_date == 0:
                fiscal_date = None
            if fiscal_date == 'None':
                fiscal_date = None
        else:
            fiscal_date = i['Fiscal/Election date']
            if fiscal_date == 0:
                fiscal_date = None
            if fiscal_date == 'None':
                fiscal_date = None
        if 'date_received' in i.keys():
            date_received = i['Contribution Received date']
            if date_received == 0:
                date_received = None
            if date_received == 'None':
                date_received = None
        else:
            date_received = '1212-12-12'
        goverlytics_id = get_goverlytics_id(i['Recipient'])
        new_dict = {
                    'recipient_id': goverlytics_id,
                    'recipient_party_id': party_id,
                    'recipient_name': i['Recipient'],
                    'contributor_prov_terr_id': cont_prov_id,
                    'contributor_name': cont_name,
                    'contributor_city': city,
                    'contributor_postal_code': post_code,
                    'date_received': date_received,
                    'fiscal_year_or_event_date': fiscal_date,
                    'part_no_of_return': part_no,
                    'contributor_type': i['Contributor type'],
                    'monetary_amount': money,
                    'non_monetary_amount': non_money,
                    }
        #print(new_dict)
        new_list_of_dicts.append(new_dict)
    return new_list_of_dicts


def get_prov_terr_id(province, district):
    #print(province)
    if not province:
        return 0
    if "NF" in province:
        province = "NL"
    try:
        if pd.notna(province):
            df = scraper_utils.divisions
            value = df.loc[df["abbreviation"] == province]['id'].values[0]
            try:
                return int(value)
            except Exception:
                return value
    except Exception as e:
        print(e)
    try:
        if pd.notna(province):
            df = scraper_utils.divisions
            value = df.loc[df["division"] == province]['id'].values[0]
            try:
                return int(value)
            except Exception:
                return value
    except Exception as e:
        print(e)
    try:
        value = get_prov_terr_id_from_district(district)
        return value
    except:
        return 0


def get_prov_terr_id_from_district(province):
    province = province.replace('–', '--')
    if pd.notna(province):
        df = districts
        value = df.loc[df["district_name"] == province]['province_territory_id'].values[0]
        try:
            return int(value)
        except Exception:
            return value


def get_party_id(party):
    party = party.split(' Party')[0]
    party_conversions = {
        "No Affiliation": 'Non-affiliated',
        'Canadian Action': 'Action',
        'Progressive Conservative': 'Conservative',
        'N.D.P.': 'New Democratic',
        'Canadian Reform Conservative Alliance': 'Reform Conservative Alliance',
        'C.H.P. of Canada': 'Christian Heritage',
        'Canadian Alliance': 'Alliance',
        'Independant': 'Independent',
        'Canada': 'Canada Party',
        'The Green': 'Green'
    }
    if party_conversions.get(party):
        party = party_conversions.get(party)
    if pd.notna(party):
        df = scraper_utils.parties
        try:
            value = df.loc[df["party"] == party]['id'].values[0]
            return int(value)
        except Exception:
            return 0


def get_goverlytics_id(recipient_name):
    recipient_id = None
    try:
        recipient_id = get_party_id(recipient_name)
    except:
        pass
    if recipient_id is None:
        try:
            first_name = recipient_name.split(', ')[1]
            first_name = first_name.split(' ')[0]
            last_name = recipient_name.split(', ')[0]
            if pd.notna(recipient_name):
                df = candidates_df
                recipient_id = df.loc[(df["name_first"] == first_name) & (df["name_last"] == last_name)]['goverlytics_id'].values[0]
        except Exception as e:
                    print(e)
    try:
        return int(recipient_id)
    except Exception:
        return 0


def get_row_data(data):
    print(data)
    row = scraper_utils.initialize_row()
    row.recipient_id = int(data['recipient_id'])
    row.recipient_party_id = int(data['recipient_party_id'])
    row.recipient_name = str(data['recipient_name'])
    row.contributor_prov_terr_id = int(data['contributor_prov_terr_id'])
    row.contributor_name = str(data['contributor_name'])
    row.contributor_city = str(data['contributor_city'])
    row.contributor_postal_code = str(data['contributor_postal_code'])
    row.date_received = str(data['date_received'])
    row.fiscal_year_or_event_date = str(data['fiscal_year_or_event_date'])
    row.part_no_of_return = str(data['part_no_of_return'])
    row.contribution_type = str(data['contributor_type'])
    row.monetary_amount = float(data['monetary_amount'])
    row.non_monetary_amount = float(data['non_monetary_amount'])
    return row


if __name__ == '__main__':
    urls = get_urls()
    data = [read_csv(url) for url in urls[:]]
    lambda_obj = lambda x: (x is not None)

    list_out = list(filter(lambda_obj, data))

    flat_ls = [item for sublist in list_out for item in sublist]
    row_data = [get_row_data(d) for d in flat_ls]
    scraper_utils.write_data(row_data)

    print('finished')
