'''
Scraper for collecting Canadian federal legislation data.
Author: Justin Tendeck
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

from tqdm import tqdm
import io
from nameparser import HumanName
import json
import psycopg2
import csv
from datetime import datetime, date
import xml.etree.ElementTree as ET
import requests
from scraper_utils import CAFedLegislationScraperUtils
from bs4 import BeautifulSoup


base_url = 'https://www.parl.ca'
xml_url_csv = 'xml_urls.csv'
table_name = 'ca_federal_legislation'

scraper_utils = CAFedLegislationScraperUtils()
crawl_delay = scraper_utils.get_crawl_delay(base_url)

# Used for switching website party to database representation
party_switcher = {
    'NDP': 'New Democratic',
    'Green Party': 'Green'
}


def get_soup(url, parser='lxml'):
    """
    Used for getting BeautifulSup representation of webpage.
    Args:
        url: URL for webpage you wish to collect data from
        parser: Type of BS parser to use.
    Returns:
        BeautifulSoup page representation
    """
    page = scraper_utils.request(url)
    return BeautifulSoup(page.content, parser)


def get_xml_urls():
    """
    Returns list of XML URLs containing basic information about every bill.
    Returns:
        XML URL list
    """
    url = f'{base_url}/legisinfo'
    soup = get_soup(url)

    try:
        session_div = soup.find('div', {'id': 'ctl00_PageContentSection_BillListingControl_BillFacetSearch_'
                                              'SessionSelector1_pnlSessions'})
        return [f'{base_url}{a.get("href")}&Language=E&download=xml' for a in session_div.find_all('a')]
    except AttributeError:
        return []


def get_english_title(terms):
    """
    Returns English title for a given term.
    Args:
        terms: term you wish to extract english title from
    Returns:
        String containing English term
    """
    titles = terms.findall('Title')

    for title in titles:
        if title.attrib['language'] == 'en':
            return title.text

    return ''


def get_event_details(event):
    """
    Used for extract event details from a given event, including chamber,
    meeting number, committee name, and status.
    Args:
        event: the event you wish to extract details from
    Returns:
        dictionary containg chamber, meeting, committee and status
    """
    chamber = event.attrib['chamber']
    date = datetime.strptime(event.attrib['date'], '%Y-%m-%d')
    meeting_num = event.attrib['meetingNumber']

    committee_xml = event.find('Committee')
    committee_full = ''

    if committee_xml:
        committee_full = get_english_title(committee_xml)

    status = get_english_title(event.find('Status'))

    return dict(chamber=chamber, date=date, meeting_number=meeting_num, committee=committee_full, status=status)


def get_current_session_number():
    """
    Used for extracting the most recent parliamentary session.
    Returns: Most recent parliamentary session number
    """
    session_page_url = f'{base_url}/LegisInfo/Home.aspx'
    soup = get_soup(session_page_url)

    session_container = soup.find('div', {
                                  'id': 'ctl00_PageContentSection_BillListingControl_BillFacetSearch_SessionSelector1_pnlSessions'})
    current_session = session_container.find('div').get_text().strip()
    return current_session.split('-')[0]


def get_tag_text(tag):
    """
    Returns all of the text from a given tag. Also includes text from all child/nested
    tags.
    Args:
        tag: tag you wish to extract text from
    Returns:
        str: all text from a given tag
    """
    txt = ''
    for childtxt in tag.itertext():
        if childtxt:
            txt += childtxt.strip() + ' '
    return txt.strip()


def get_bill_summary_and_text(bill_file_url):
    """
    Extracts bill summary and text from the webpage of a given bill URL.
    Args:
        bill_file_url: URL for the a given bill's parl.ca webpage
    Returns: dict containing bill_summary and bill_text
    """
    bill_data = {'bill_text': '', 'bill_summary': ''}
    if not bill_file_url:
        return bill_data

    soup = get_soup(bill_file_url)
    xml_url_path = soup.find(
        'a', {'class': 'btn btn-export-xml hidden-xs'}).get('href')
    xml_url = base_url + xml_url_path

    page = scraper_utils.request(xml_url)
    root = ET.fromstring(page.content)

    bill_data['bill_summary'] = get_tag_text(
        root.find('Introduction').find('Summary'))
    bill_data['bill_text'] = get_tag_text(root.find('Body'))

    return bill_data


def scrape(xml_url):
    """
    Entry point for scraping. Bill data comes from an XML sheet that lists a set of 
    bills along with basic information. Since some datapoints are missing from the main
    XML sheet, additional data collection may occur, including data collection from an
    individual bill's information page.
    Args:
        xml_url: XML URL containing information about a list of bill
    Returns:
        List of rows containing bill information
    """
    page = scraper_utils.request(xml_url)
    root = ET.fromstring(page.content)

    current_session_number = get_current_session_number()

    bill_lst = []
    for bill in tqdm(root.findall('Bill'), "Bill scrapin'"):

        parl_session = bill.find('ParliamentSession')
        parl_number = parl_session.attrib["parliamentNumber"]

        if parl_number != current_session_number:
            continue

        row = scraper_utils.initialize_row()

        bill_id = bill.attrib['id']
        introduced_date = datetime.strptime(
            bill.find('BillIntroducedDate').text, '%Y-%m-%dT%H:%M:%S')

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
        party_abbreviation = get_english_title(
            affiliation.find('PoliticalParty').find('abbreviation'))

        pm_details = bill.find('PrimeMinister')
        pm_full_name = pm_details.find('Person').find('FullName').text
        pm_party = get_english_title(pm_details.find('PoliticalParty'))
        pm_party_abbreviation = get_english_title(
            pm_details.find('PoliticalParty').find('abbreviation'))

        statute = bill.find('Statute')
        statute_year = statute.find('Year').text
        statute_chapter = statute.find('Chapter').text

        publications = bill.find('Publications')
        all_publications = publications.findall('Publication')
        publication_lst = []
        for pub in all_publications:
            publication_lst.append(get_english_title(pub))

        bill_file_url = ''
        if len(all_publications) > 0:
            latest_publication = all_publications[-1]
            publication_files = latest_publication.find(
                'PublicationFiles').findall('PublicationFile')

            for pf in publication_files:
                if pf.attrib['language'] == 'en':
                    bill_file_url = f"https:{pf.attrib['relativePath']}"
        bill_file_details = get_bill_summary_and_text(bill_file_url)

        events = bill.find('Events')

        last_major_event_xml = events.find('LastMajorStageEvent')
        last_major_event = get_event_details(
            last_major_event_xml.find('Event'))

        progress = last_major_event_xml.find('Progress').text

        legislative_events_xml = events.find('LegislativeEvents')
        legislative_events = [get_event_details(
            event) for event in legislative_events_xml.findall('Event')]

        committees = []
        for le in legislative_events:
            if 'committee' in le and le['committee'] != '':
                committees.append(
                    {'chamber': le['chamber'], 'committee': le['committee']})

        row.committees = committees
        row.goverlytics_id = f'CAFED_{session}_{bill_number}'
        row.source_url = f'{base_url}/LegisInfo/BillDetails.aspx?Language=E&billId={bill_id}'
        row.source_id = bill_id
        row.bill_name = bill_number
        row.session = session
        row.date_introduced = introduced_date
        row.chamber_origin = bill_number[0]
        row.bill_type = bill_type
        row.bill_title = bill_title_short if bill_title_short else ''
        row.current_status = progress
        ps_id = scraper_utils.get_legislator_id(
            name_last=last_name, name_first=first_name)
        row.principal_sponsor_id = ps_id
        row.principal_sponsor = full_name
        row.bill_text = bill_file_details['bill_text']
        row.bill_description = bill_title
        row.bill_summary = bill_file_details['bill_summary']
        row.actions = legislative_events
        try:
            row.province_territory_id = scraper_utils.get_attribute(
                'legislator', 'goverlytics_id', ps_id, 'province_territory_id')
            p_t = scraper_utils.get_attribute(
                'legislator', 'goverlytics_id', ps_id, 'province_territory')
            row.province_territory = p_t
            row.region = scraper_utils.get_region(p_t)
        except:
            print(f'Could not find prov, terr data for: {full_name}')
            row.province_territory = None
            row.province_territory_id = None
            row.region = None
        row.sponsor_affiliation = sponsor_affiliation
        row.sponsor_gender = sponsor_gender
        row.pm_name_full = pm_full_name
        row.pm_party = party_switcher[party] if party in party_switcher else party
        row.pm_party_id = scraper_utils.get_attribute(
            'party', 'party', row.pm_party)
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

    xml_data = [scrape(xml_url) for xml_url in xml_urls]
    # scrape() returns a list so we need to coalesce data into one large list
    xml_data = [item for sublist in xml_data for item in sublist]

    if len(xml_data) > 0:
        scraper_utils.write_data(xml_data)

    print('Complete!')
