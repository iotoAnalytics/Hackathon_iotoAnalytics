import sys
import os
from datetime import datetime
from pathlib import Path
import time
from multiprocessing import Pool
from pandas import DataFrame
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

from scraper_utils import CandidateElectionFinancesScraperUtils
from database import CursorFromConnectionFromPool
from bs4 import BeautifulSoup
import pandas as pd
import dateutil.parser as dparser

NODES_TO_ROOT = 4
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

PATH = "../../../web_drivers/chrome_win_91.0.4472.19/chromedriver.exe"
browser = webdriver.Chrome(PATH)

# https://www.elections.ca/WPAPPS/WPF/EN/Home/Index
COUNTRY = 'ca'
TABLE = 'ca_candidate_election_finances'
MAIN_URL = 'https://www.elections.ca'
ELECTION_FINANCES_URL = MAIN_URL + '/WPAPPS/WPF/EN/Home/Index'
THREADS_FOR_POOL = 12

#
scraper_utils = CandidateElectionFinancesScraperUtils(COUNTRY, TABLE)
crawl_delay = scraper_utils.get_crawl_delay(MAIN_URL)

with CursorFromConnectionFromPool() as cur:
    try:
        query = 'SELECT * FROM ca_candidate_election_details'
        cur.execute(query)
        candidate_election_details = cur.fetchall()

        query = 'SELECT * FROM ca_candidates'
        cur.execute(query)
        candidates = cur.fetchall()

        query = 'SELECT * FROM ca_elections'
        cur.execute(query)
        elections_table = cur.fetchall()

    except Exception as e:
        sys.exit(
            f'An exception occurred retrieving tables from database:\n{e}')

    candidates_election = pd.DataFrame(candidate_election_details)
    candidates_table = pd.DataFrame(candidates)
    elections = pd.DataFrame(elections_table)


def get_data():
    data = []
    options1 = get_first_list_of_options()
    for option in options1:
        second_options = get_second_list_of_options(option, options1)
        for o_2 in second_options:
            data.extend(get_candidate_pages(option, o_2))
    return data


def get_first_list_of_options():
    option_list = []
    browser.get(ELECTION_FINANCES_URL)
    select = browser.find_element_by_tag_name('select')
    select.click()
    time.sleep(3)
    options = browser.find_elements_by_tag_name('option')
    for o_1 in options:
        if "Select" not in o_1.text:
            option_list.append(o_1.text)
    option_list = [x for x in option_list if x]
    return option_list


def get_second_list_of_options(option, option_list):
    option_2_list = []
    browser.get(ELECTION_FINANCES_URL)
    select = browser.find_element_by_tag_name('select')
    select.click()
    time.sleep(3)
    options = browser.find_elements_by_tag_name('option')
    for o in options:
        if option in o.text:
            o.click()
    time.sleep(3)
    select_2 = browser.find_elements_by_tag_name('select')
    select_2[1].click()
    time.sleep(3)
    options_2 = browser.find_elements_by_tag_name('option')
    for o_2 in options_2:
        if "Select" not in o_2.text:
            option_2_list.append(o_2.text)
    option_2_list = [x for x in option_2_list if x not in option_list]
    option_2_list = [x for x in option_2_list if x]
    return option_2_list


def get_candidate_pages(option, o_2):
    candidate_election_finances_list = []
    browser.get(ELECTION_FINANCES_URL)
    select = browser.find_element_by_tag_name('select')
    select.click()
    time.sleep(3)
    options = browser.find_elements_by_tag_name('option')
    for o in options:
        if option in o.text:
            o.click()
    time.sleep(3)
    select_2 = browser.find_elements_by_tag_name('select')
    select_2[1].click()
    time.sleep(3)
    options_2 = browser.find_elements_by_tag_name('option')
    for o in options_2:
        if o_2 in o.text:
            o.click()
            time.sleep(2)
    select_3 = browser.find_element_by_id('reportTypeList')
    select_3.click()
    options_3 = browser.find_elements_by_tag_name('option')
    for o_3 in options_3:
        if "Campaign Returns" in o_3.text:
            o_3.click()
            time.sleep(2)
    search_button = browser.find_element_by_id('SearchButton')
    search_button.click()
    current_url = browser.current_url
    candidate_list = search_candidates(current_url)
    candidate_election_finances_list.extend(get_candidate_election_details(candidate_list))
    return candidate_election_finances_list


def search_candidates(url):
    candidate_list = []
    browser.get(url)
    browser.find_element_by_id('button3').click()
    time.sleep(3)
    browser.find_element_by_xpath('//*[@id="SelectedClientIds"]/option[1]').click()
    #browser.find_element_by_id('SelectAllCandidates').click()
    time.sleep(3)
    browser.find_element_by_id('SearchSelected').click()

    while True:
        election = browser.find_element_by_id('eventname').text
        candidate = browser.find_element_by_id('ename1').text
        party_district = browser.find_element_by_id('partydistrict1').text
        browser.find_element_by_id('SelectedPart').click()
        time.sleep(1)
        options = browser.find_elements_by_tag_name('option')
        for option in options:
            if 'Part  6' in option.text:
                option.click()
            elif 'Campaign Financial Summary' in option.text:
                option.click()
        browser.find_element_by_id('ReportOptions').click()
        try:
            date = browser.find_element_by_class_name('date').text
            dt_object = datetime.strptime(date, '%b %d, %Y')
            date_of_return = dt_object.strftime("%Y-%m-%d")
        except:
            date_of_return = "1212-12-12"
        candidate_info = {'election': election, 'name': candidate, 'date_of_return': date_of_return,
                          'party_district': party_district}
        candidate_list.append(candidate_info)

        try:
            next_candidate = browser.find_element_by_id('nextpagelink_top')
            next_candidate.click()
        except Exception as e:
            break
        time.sleep(1)

    return candidate_list


def get_candidate_election_details(candidate_list):
    completed_list = []
    for c in candidate_list:
        party = c['party_district'].split('/')[0]
        party_id = get_party_id(party)
        name = c['name']
        gov_id = get_goverlytics_id(name, party_id)
        election_id = get_election_id(c['election'])
        candidate_election_id = get_candidate_election_id(gov_id, party_id, election_id)
        candidate = {'candidate_election_id': candidate_election_id, 'date_of_return': c['date_of_return']}
        completed_list.append(candidate)
    return completed_list


def get_candidate_election_id(gov_id, party_id, election_id):
    if pd.notna(gov_id):
        df = candidates_election
        try:
            ce_id = df.loc[(df["candidate_id"] == gov_id) & (df["party_id"] == party_id) &
                           (df["election_id"] == election_id)]['id'].values[0]
        except:
            ce_id = 0
    try:
        return int(ce_id)
    except Exception:
        return 0


def get_goverlytics_id(name, party):
    last_name = name.split(',')[0].capitalize()
    first_name = name.split(', ')[1].capitalize()
    if pd.notna(name):
        df = candidates_table
        try:
            recipient_id = df.loc[(df["name_first"].str.contains(first_name)) & (df["name_last"] == last_name) &
                                  (df["current_party_id"] == party)]['goverlytics_id'].values[0]
        except:
            recipient_id = 0
    try:
        return int(recipient_id)
    except Exception:
        return 0


def get_party_id(party):
    try:
        party = party.split(' Party')[0]
        party = party.strip()
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
            'The Green': 'Green',
            'Parti Rhinocéros': 'Rhinoceros'
        }
        if party_conversions.get(party):
            party = party_conversions.get(party)
        if pd.notna(party):
            df = scraper_utils.parties
            try:
                value = df.loc[df["party"] == party]['id'].values[0]
                return int(value)
            except Exception as e:
                print(e)
                return 0
    except:
        return 0


def get_election_id(election):
    try:
        if 'by-election' in election.lower():
            election = election.split(' (')[0]
            try:
                date = dparser.parse(election, fuzzy=True)
                date_name = date.strftime("%Y_%m_%d")
                election_name = 'by_election_' + date_name
            except Exception as e:
                print(e)
            if pd.notna(election):
                df = elections
                value = df.loc[df['election_name'].str.contains(election_name)]['id'].values[0]
                try:
                    return int(value)
                except Exception as e:
                    return value
        if 'general' in election.lower():
            general_elections = {
                '50th': '50',
                '49th': '49',
                '48th': '48',
                '47th': '47',
                '46th': '46',
                '45th': '45',
                '44th': '44',
                '43rd': '43',
                '42nd': '42',
                '41st': '41',
                '40th': '40',
                '39th': '39',
                '38th': '38',
                '37th': '37',
                '36th': '36'
            }
            election = election.split(' ')[0].lower()
            e_number = general_elections.get(election)
            election_name = e_number + '_general_election'
            if pd.notna(election):
                df = elections
                value = df.loc[df['election_name'].str.contains(election_name)]['id'].values[0]
                try:
                    return int(value)
                except Exception as e:
                    return value
    except:
        return 0


def get_row_data(data):
    row = scraper_utils.initialize_row()
    row.candidate_election_id = int(data['candidate_election_id'])
    row.date_of_return = str(data['date_of_return'])
    return row


if __name__ == '__main__':
    data = get_data()
    row_data = [get_row_data(d) for d in data]
    scraper_utils.write_data(row_data)
    print('finished')
