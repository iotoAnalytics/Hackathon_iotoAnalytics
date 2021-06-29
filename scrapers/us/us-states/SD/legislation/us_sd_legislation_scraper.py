# Unavailable data - bill_description, bill_summary

import io
import multiprocessing
import os
import re
import sys
from datetime import datetime
from functools import partial

import pdfplumber
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool
from pathlib import Path
from pprint import pprint
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from us_sd_legislation_utils import BILL_TYPE_FULL, CHAMBER_FULL
from scraper_utils import USStateLegislationScraperUtils

DEBUG_MODE = False

STATE_ABBREVIATION = 'SD'
DATABASE_TABLE_NAME = 'us_sd_legislation'
LEGISLATOR_TABLE_NAME = 'us_sd_legislators'
BASE_URL = 'https://sdlegislature.gov/'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION, DATABASE_TABLE_NAME, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_session_members_data(session_id):
    response = scraper_utils.request(f'https://sdlegislature.gov/api/SessionMembers/Session/{session_id}').json()
    scraper_utils.crawl_delay(crawl_delay)
    return response

def get_topic_index(session_id):
    response = scraper_utils.request(f'https://sdlegislature.gov/api/Keywords?SessionId={session_id}').json()
    scraper_utils.crawl_delay(crawl_delay)
    return response

def get_current_session_data():
    response = scraper_utils.request('https://sdlegislature.gov/api/Sessions/').json()
    scraper_utils.crawl_delay(crawl_delay)
    
    for session in response:
        if session['CurrentSession']:
            return session

def get_current_session_id(session_data):
    return session_data['SessionId']

def get_current_session_bills_data(session_id):
    response = scraper_utils.request(f'https://sdlegislature.gov/api/Bills/Session/{session_id}').json()
    scraper_utils.crawl_delay(crawl_delay)
    return response

def get_session_number(session_id):
    response = scraper_utils.request(f'https://sdlegislature.gov/api/Sessions/{session_id}').json()
    scraper_utils.crawl_delay(crawl_delay)
    session_number = response['SessionNumber']
    return session_number

def init_session(bill_data, session_number):
    bill_data['Session'] = str(session_number)

def set_bill_data(bill_data, search_table=None):
    row = scraper_utils.initialize_row()
    bill_id = _get_bill_id(bill_data)

    try:
        _set_source_id(row, bill_data)
        _set_bill_name(row, bill_data)
        _set_session(row, bill_data)
        _set_date_introduced(row, bill_data)
        _set_source_url(row, bill_id)
        _set_chamber_origin(row, bill_data)
        _set_committees(row, bill_data)
        _set_bill_type(row, bill_data)
        _set_bill_title(row, bill_data)
        _set_current_status(row, bill_id)
        _set_principal_sponsor(row, bill_data, search_table['legislator_members_data'])
        _set_sponsors(row, bill_data, search_table['legislator_members_data'])
        _set_cosponsors(row, bill_data, search_table['legislator_members_data'])
        _set_bill_text(row, bill_id)
        _set_actions(row, bill_id)
        _set_votes(row, bill_id)
        _set_source_topic(row, bill_data, search_table['topic_index'])
        _set_goverlytics_id(row)
    except Exception:
        print(f'Error with: {bill_id}')

    return row

def _get_bill_id(bill_data):
    return bill_data['BillId']

def _set_source_id(row, bill_data):
    source_id = bill_data['BillId']
    row.source_id = source_id

def _set_bill_name(row, bill_data):
    bill_name = bill_data['BillType'] + bill_data['BillNumber']
    row.bill_name = bill_name

def _set_session(row, bill_data):
    session = bill_data['Session']
    row.session = session

def _set_date_introduced(row, bill_data):
    date_introduced = bill_data['Introduced']
    row.date_introduced = _format_date(date_introduced)

def _format_date(date):
    # Convert the following format:
    # 2021-01-25T10:00:00-06:00 -> 2021-01-25
    if date:= re.search(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', date):
        return datetime.strptime(date.group(0), r'%Y-%m-%d')

def _set_source_url(row, bill_id):
    source_url = f'https://sdlegislature.gov/Session/Bill/{bill_id}'
    row.source_url = source_url

def _set_chamber_origin(row, bill_data):
    chamber_origin_abvr = bill_data['BillType'][0]
    chamber_origin = CHAMBER_FULL.get(chamber_origin_abvr)        
    row.chamber_origin = chamber_origin

def _set_committees(row, bill_data):
    committees = []
    
    if session_committee_id:= bill_data['SessionCommitteeId']:
        committee = {
            'chamber': _get_committee_chamber(session_committee_id, 'session'),
            'committee': _get_committee_name(session_committee_id, 'session')
        }
        committees.append(committee)

    if interim_committee_id:= bill_data['InterimCommitteeId']:
        committee = {
            'chamber': _get_committee_chamber(interim_committee_id, 'interim'),
            'committee': _get_committee_name(interim_committee_id, 'interim')
        }
        committees.append(committee)

    row.committees = committees

def _get_committee_data(committee_id, committee_type):
    accepted_types = ['session', 'interim']

    if committee_type not in accepted_types:
        raise Exception(
            f'Error: committee type must be one of the following: {accepted_types}')

    if committee_type == 'session':
        response = scraper_utils.request(f'https://sdlegislature.gov/api/SessionCommittees/{committee_id}')
    if committee_type == 'interim':
        response = scraper_utils.request(f'https://sdlegislature.gov/api/InterimCommittees/{committee_id}')
    
    scraper_utils.crawl_delay(crawl_delay)
    committee_data = response.json()
    return committee_data

def _get_committee_name(committee_id, committee_type):
    committee_data = _get_committee_data(committee_id, committee_type)
    return committee_data['Committee']['FullName']

def _get_committee_chamber(committee_id, committee_type):
    committee_data = _get_committee_data(committee_id, committee_type)
    chamber = committee_data['Body']
    return CHAMBER_FULL.get(chamber)

def _set_bill_type(row, bill_data):
    bill_type_abrv = bill_data['BillType']
    bill_type = BILL_TYPE_FULL.get(bill_type_abrv)
    row.bill_type = bill_type

def _set_bill_title(row, bill_data):
    bill_title = bill_data['Title']
    row.bill_title = bill_title

def _set_current_status(row, bill_id):
    response = scraper_utils.request(f'https://sdlegislature.gov/api/Bills/ActionLog/{bill_id}').json()
    scraper_utils.crawl_delay(crawl_delay)
    
    if response:
        current_status = response[-1]['StatusText']
        row.current_status = current_status

def _set_principal_sponsor(row, bill_data, legislator_members_data):
    sponsors_sid = bill_data['PrimeSponsors']
    is_prime_sponsor = len(sponsors_sid) == 1

    if is_prime_sponsor:
        sponsor_sid = sponsors_sid[0]
        row.principal_sponsor_id = scraper_utils.get_legislator_id(source_id=sponsor_sid)
        row.principal_sponsor = _get_sponsor_name(sponsor_sid, legislator_members_data)

def _get_sponsor_id(source_id):
    gov_id = scraper_utils.get_legislator_id(source_id=source_id)
    return gov_id

def _get_sponsor_name(source_id, legislator_members_data):
    for member in legislator_members_data:
        if member['SessionMemberId'] == source_id:
            full_name = f"{member['FirstName']} {member['LastName']}"
            return full_name

def _set_sponsors(row, bill_data, legislator_members_data):
    sponsors_sid = bill_data['PrimeSponsors']
    has_more_than_one_prime_sponsors = len(sponsors_sid) > 1

    sponsors_id = []
    sponsors = []

    # Set prime sponsors as sponsors if there are more than one prime sponsors 
    if has_more_than_one_prime_sponsors:
        for sponsor_sid in sponsors_sid:
            sponsors_id.append(scraper_utils.get_legislator_id(source_id=sponsor_sid))
            sponsors.append(_get_sponsor_name(sponsor_sid, legislator_members_data))
        row.sponsors_id = sponsors_id
        row.sponsors = sponsors

    # Skip committee if cosponsors has already been listed (above) yet to avoid duplication
    if bill_data['Sponsors']:
        return

    # Set committee as a sponsor
    if session_committee_id:= bill_data['SessionCommitteeId']:
        committee_name = _get_committee_name(session_committee_id, 'session')
    if interim_committee_id:= bill_data['InterimCommitteeId']:
        committee_name = _get_committee_name(interim_committee_id, 'interim')
    sponsors.append(committee_name)

    row.sponsors_id += sponsors_id
    row.sponsors += sponsors

def _set_cosponsors(row, bill_data, legislator_members_data):
    cosponsors_id = []
    cosponsors = []

    prime_sponsors_sid = bill_data['PrimeSponsors']

    if sponsors_data:= bill_data['Sponsors']:
        for sponsor_sid in sponsors_data:
            # Don't add prime sponsors as cosponsors
            if sponsor_sid in prime_sponsors_sid:
                continue

            cosponsors_id.append(scraper_utils.get_legislator_id(source_id=sponsor_sid))
            cosponsors.append(_get_sponsor_name(sponsor_sid, legislator_members_data))

    row.cosponsors_id = cosponsors_id
    row.cosponsors = cosponsors

def _set_bill_text(row, bill_id):
    response = scraper_utils.request(f'https://sdlegislature.gov/api/Bills/Versions/{bill_id}').json()
    scraper_utils.crawl_delay(crawl_delay)

    document_id = response[-1]['DocumentId']
    document_url = f'https://mylrc.sdlegislature.gov/api/Documents/{document_id}.pdf'

    response = requests.get(document_url, stream=True)
    pdf = pdfplumber.open(io.BytesIO(response.content))

    bill_text = ''
    for page in pdf.pages:
        if page_text:= page.extract_text():
            bill_text += page_text

    row.bill_text = bill_text

def _set_actions(row, bill_id):
    actions = []
    
    response = scraper_utils.request(f'https://sdlegislature.gov/api/Bills/ActionLog/{bill_id}').json()
    scraper_utils.crawl_delay(crawl_delay)
    
    for action_data in response:
        if action_data['ActionCommittee']:
            action = {
                'date': _format_date(action_data['ActionDate']),
                'action_by': CHAMBER_FULL.get(action_data['ActionCommittee']['Body']),
                'description': action_data['Description']
            }
            actions.append(action)
        
    row.actions = actions

def _set_votes(row, bill_id):
    votes = []

    votes_ias  = _get_votes_id_and_status(bill_id)
    for vote_ias in votes_ias:
        vote_data = _get_votes_data(vote_ias)
        votes.append(vote_data)

    row.votes = votes

def _get_votes_id_and_status(bill_id):
    votes_id_and_status = []
    
    response = scraper_utils.request(f'https://sdlegislature.gov/api/Bills/ActionLog/{bill_id}').json()
    scraper_utils.crawl_delay(crawl_delay)

    for action_data in response: 
        try:
            vote_id = action_data['Vote']['VoteId']
            vote_status = action_data['Result']
            votes_id_and_status.append((vote_id, vote_status))
        except Exception:
            pass

    return votes_id_and_status

def _get_votes_data(vote_ias):
    votes_data = {}

    vote_id, vote_status = vote_ias

    response = scraper_utils.request(f'https://sdlegislature.gov/api/Votes/{vote_id}').json()
    scraper_utils.crawl_delay(crawl_delay)

    action_log = response['actionLog']
    roll_calls = response['RollCalls']

    votes_data['date'] = _format_date(action_log['ActionDate'])
    votes_data['description'] = action_log['StatusText']
    votes_data['yea'] = response['Yeas']
    votes_data['nay'] = response['Nays']
    votes_data['nv'] = response['Excused'] + response['NotVoting']
    votes_data['absent'] = response['Absent']
    votes_data['total'] = votes_data['yea'] + votes_data['nay'] + votes_data['nv'] + votes_data['absent']
    votes_data['passed'] = vote_status == 'P'
    votes_data['chamber'] = action_log['FullName'].split()[0]
    
    members_vote = []
    for member_data in roll_calls:
        vote = {
            'goverlytics_id': scraper_utils.get_legislator_id(source_id=member_data['SessionMemberId']),
            'legislator': member_data['UniqueName'],
            'votet': member_data['Vote1'].lower()
        }
        members_vote.append(vote)
    votes_data['votes'] = members_vote

    return votes_data

def _set_source_topic(row, bill_data, topic_index):
    topics_id = bill_data['Keywords']
    source_topic = []

    for topic_id in topics_id:
        topic = _get_topic(topic_id, topic_index)
        source_topic.append(topic)
    else:
        # Change to string format
        source_topic = ', '.join(source_topic)    

    row.source_topic = source_topic

def _get_topic(topic_id, topic_index):
    for topic in topic_index:
        if topic['KeywordId'] == topic_id:
            return topic['Keyword']

def _set_goverlytics_id(row):
    goverlytics_id = f'{STATE_ABBREVIATION}_{row.session}_{row.bill_name}'
    row.goverlytics_id = goverlytics_id

def main():
    print('\nSCRAPING SOUTH DAKOTA LEGISLATION\n')

    # Get session data and IDs
    print(DEBUG_MODE and 'Retrieving session data and IDs...\n' or '', end='')
    session_data = get_current_session_data()
    session_id = get_current_session_id(session_data)
    session_bills_data = get_current_session_bills_data(session_id)

    # Populate search_table
    print(DEBUG_MODE and 'Populating search tables for easier lookup...\n' or '', end='')
    search_table = {
        'legislator_members_data': get_session_members_data(session_id),
        'topic_index': get_topic_index(session_id) 
    }

    # Initialize sessions into members data
    print(DEBUG_MODE and 'Initializing sessions into members data...\n' or '', end='')
    session_number = get_session_number(session_id)
    for bill_data in session_bills_data:
        init_session(bill_data, session_number)

    # Set fields
    print(DEBUG_MODE and 'Setting fields of legislation rows...\n' or '', end='')
    with Pool(NUM_POOL_PROCESSES) as pool:
        data = list(tqdm(pool.imap(partial(set_bill_data, search_table=search_table), session_bills_data)))

    # Write to database
    print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
    if not DEBUG_MODE:
        scraper_utils.write_data(data)

if __name__ == '__main__':
    main()