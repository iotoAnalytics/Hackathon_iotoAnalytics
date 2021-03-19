import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

from bs4 import BeautifulSoup
from legislator_scraper_utils import CadFedLegislationScraperUtils
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date
import csv
import psycopg2
import json


from testytester import yo
yo()

base_url = 'https://www.parl.ca'
xml_url_csv = 'xml_urls.csv'
table_name = 'cdn_federal_legislation'

scraper_utils = CadFedLegislationScraperUtils()



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


def parse_xml_data(xml_url):
    page = requests.get(xml_url)
    root = ET.fromstring(page.content)

    bill_lst = []
    for bill in root.findall('Bill'):
        bill_id = bill.attrib['id']
        introduced_date = datetime.strptime(bill.find('BillIntroducedDate').text, '%Y-%m-%dT%H:%M:%S')

        parl_session = bill.find('ParliamentSession')
        session = f'{parl_session.attrib["parliamentNumber"]}-{parl_session.attrib["sessionNumber"]}'

        bill_number = bill.find('BillNumber')
        bill_number = f'{bill_number.attrib["prefix"]}-{bill_number.attrib["number"]}'

        bill_title = get_english_title(bill.find('BillTitle'))
        bill_title_short = get_english_title(bill.find('ShortTitle'))

        bill_type = get_english_title(bill.find('BillType'))

        affiliation = bill.find('SponsorAffiliation')
        sponsor_affiliation = get_english_title(affiliation)

        sponsor_details = affiliation.find('Person')
        full_name = sponsor_details.find('FullName').text
        first_name = sponsor_details.find('FirstName').text
        middle_name = sponsor_details.find('MiddleName').text if sponsor_details.find('MiddleName').text else ''
        last_name = sponsor_details.find('LastName').text
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

        bill_lst.append(dict(bill_number=bill_number, bill_id=bill_id, introduction_date=introduced_date,
                             session=session, bill_title=bill_title, bill_title_short=bill_title_short,
                             bill_type=bill_type, sponsor_affiliation=sponsor_affiliation, sponsor_id=sponsor_id,
                             sponsor_full_name=full_name, sponsor_first_name=first_name, sponsor_middle_name=middle_name,
                             sponsor_last_name=last_name, sponsor_gender=sponsor_gender, sponsor_party=party,
                             sponsor_party_abbr=party_abbreviation, pm_full_name=pm_full_name, pm_party=pm_party,
                             pm_party_abbreviation=pm_party_abbreviation, statute_year=statute_year,
                             statute_chapter=statute_chapter, publications=publication_lst,
                             last_major_event=last_major_event, progress=progress,
                             legislative_events=legislative_events))

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

    xml_urls = xml_urls[:1] if len(xml_urls) > 0 else []

    xml_data = [parse_xml_data(xml_url) for xml_url in xml_urls]
    xml_data = [item for sublist in xml_data for item in sublist]

    if len(xml_data) > 0:
        scraper_utils.insert_legislation_data_into_db(xml_data)