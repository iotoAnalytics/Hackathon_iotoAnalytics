import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

from bs4 import BeautifulSoup
from legislation_scraper_utils import CadFedLegislationScraperUtils
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date
import csv
import psycopg2
import json
from nameparser import HumanName

base_url = 'https://www.parl.ca'
xml_url_csv = 'xml_urls.csv'
table_name = 'cdn_federal_legislation'

scraper_utils = CadFedLegislationScraperUtils()
party_switcher = {
    'NDP': 'New Democratic',
    'Green Party': 'Green'
}

def get_soup(url, parser='lxml'):
    page = requests.get(url)
    return BeautifulSoup(page.content, parser)


def get_xml_urls():
    url = f'{base_url}/legisinfo'
    soup = get_soup(url)

    try:
        session_div = soup.find('div', {'id': 'ctl00_PageContentSection_BillListingControl_BillFacetSearch_'
                                              'SessionSelector1_pnlSessions'})
        return [f'{base_url}{a.get("href")}&Language=E&download=xml' for a in session_div.find_all('a')]
    except AttributeError:
        return []


def get_english_title(terms):
    titles = terms.findall('Title')

    for title in titles:
        if title.attrib['language'] == 'en':
            return title.text

    return ''


def get_event_details(event):
    chamber = event.attrib['chamber']
    date = datetime.strptime(event.attrib['date'], '%Y-%m-%d')
    meeting_num = event.attrib['meetingNumber']

    committee_xml = event.find('Committee')
    committee_acr = ''
    committee_full = ''

    if committee_xml:
        committee_acr = committee_xml.attrib['accronym'] if 'accronym' in committee_xml.attrib else ''
        committee_full = get_english_title(committee_xml)

    committee = dict(committee=committee_full, committee_acronym=committee_acr)

    status = get_english_title(event.find('Status'))

    return dict(chamber=chamber, date=date, meeting_number=meeting_num, committee=committee, status=status)

def get_current_session_number():
    session_page_url = f'{base_url}/LegisInfo/Home.aspx'
    soup = get_soup(session_page_url)

    session_container = soup.find('div', {'id': 'ctl00_PageContentSection_BillListingControl_BillFacetSearch_SessionSelector1_pnlSessions'})
    current_session = session_container.find('div').get_text().strip()
    return current_session.split('-')[0]

def parse_xml_data(xml_url):
    page = requests.get(xml_url)
    root = ET.fromstring(page.content)

    current_session_number = get_current_session_number()

    bill_lst = []
    for bill in root.findall('Bill'):

        parl_session = bill.find('ParliamentSession')
        parl_number = parl_session.attrib["parliamentNumber"]

        if parl_number != current_session_number:
            continue

        # print(f'CURRENT SESSION: {current_session_number} | CHECKING SESSION: {parl_number}')

        row = scraper_utils.initialize_row()

        bill_id = bill.attrib['id']
        introduced_date = datetime.strptime(bill.find('BillIntroducedDate').text, '%Y-%m-%dT%H:%M:%S')

        session = f'{parl_number}-{parl_session.attrib["sessionNumber"]}'

        bill_number = bill.find('BillNumber')
        bill_number = f'{bill_number.attrib["prefix"]}-{bill_number.attrib["number"]}'

        bill_title = get_english_title(bill.find('BillTitle'))
        bill_title_short = get_english_title(bill.find('ShortTitle'))

        bill_type = get_english_title(bill.find('BillType'))

        affiliation = bill.find('SponsorAffiliation')
        sponsor_affiliation = get_english_title(affiliation)

        sponsor_details = affiliation.find('Person')
        full_name = sponsor_details.find('FullName').text
        hn = HumanName(full_name)
        first_name = hn.first
        middle_name = hn.middle
        last_name = hn.last
        sponsor_gender = sponsor_details.attrib['Gender']
        sponsor_id = sponsor_details.attrib['id']
        party = get_english_title(affiliation.find('PoliticalParty'))
        party_abbreviation = get_english_title(affiliation.find('PoliticalParty').find('abbreviation'))

        pm_details = bill.find('PrimeMinister')
        pm_full_name = pm_details.find('Person').find('FullName').text
        pm_party = get_english_title(pm_details.find('PoliticalParty'))
        pm_party_abbreviation = get_english_title(pm_details.find('PoliticalParty').find('abbreviation'))

        statute = bill.find('Statute')
        statute_year = statute.find('Year').text
        statute_chapter = statute.find('Chapter').text

        publications = bill.find('Publications')
        publication_lst = []
        for pub in publications.findall('Publication'):
            publication_lst.append(get_english_title(pub))

        events = bill.find('Events')

        last_major_event_xml = events.find('LastMajorStageEvent')
        last_major_event = get_event_details(last_major_event_xml.find('Event'))

        progress = last_major_event_xml.find('Progress').text

        legislative_events_xml = events.find('LegislativeEvents')
        legislative_events = [get_event_details(event) for event in legislative_events_xml.findall('Event')]
        
        row.goverlytics_id = f'CADFED_{session}_{bill_number}'

        # print(row.goverlytics_id)

        row.source_id = bill_id
        row.bill_name = bill_number
        row.session = session
        row.date_introduced = introduced_date
        row.chamber_origin = bill_number[0]
        row.bill_type = bill_type
        row.bill_title = bill_title
        row.current_status = progress
        ps_id = scraper_utils.get_legislator_id(name_last=last_name, name_first=first_name)
        row.principal_sponsor_id = ps_id
        row.principal_sponsor = full_name
        #row.bill_text = 
        row.bill_description = bill_title_short if bill_title_short else ''
        # row.bill_summary = 
        row.actions = legislative_events
        # row.votes = 
        # row.source_topic = 
        # row.topic =
        try:
            row.province_territory_id = scraper_utils.get_attribute('legislator', 'goverlytics_id', ps_id, 'province_territory_id')
            row.province_territory = scraper_utils.get_attribute('legislator', 'goverlytics_id', ps_id, 'province_territory')
        except:
            print(f'Could not find prov, terr data for: {full_name}')
            row.province_territory = None
            row.province_territory_id = None
        # row.province_territory = scraper_utils.get_attribute('legislator', 'name_full', full_name, 'province_territory')
        row.sponsor_affiliation = sponsor_affiliation
        row.sponsor_gender = sponsor_gender
        row.pm_name_full = pm_full_name
        row.pm_party = party_switcher[party] if party in party_switcher else party
        row.pm_party_id = scraper_utils.get_attribute('party', 'party', row.pm_party)
        row.statute_year = statute_year
        row.statute_chapter = statute_chapter
        row.publications = publication_lst
        row.last_major_event = last_major_event

        bill_lst.append(row)

    return bill_lst


if __name__ == '__main__':

    xml_urls = []

    try:
        with open(xml_url_csv) as f:
            reader = csv.reader(f)
            xml_urls = list(reader)[0]
    except FileNotFoundError:
        print(f'{xml_url_csv} does not exist. Scraping urls...')

        xml_urls = get_xml_urls()
        with open(xml_url_csv, 'w') as result_file:
            wr = csv.writer(result_file)
            wr.writerow(xml_urls)

    # xml_urls = xml_urls[:1] if len(xml_urls) > 0 else []
    xml_urls = xml_urls if len(xml_urls) > 0 else []

    xml_data = [parse_xml_data(xml_url) for xml_url in xml_urls]
    xml_data = [item for sublist in xml_data for item in sublist]

    if len(xml_data) > 0:
        scraper_utils.insert_legislation_data_into_db(xml_data)

    print('Complete!')