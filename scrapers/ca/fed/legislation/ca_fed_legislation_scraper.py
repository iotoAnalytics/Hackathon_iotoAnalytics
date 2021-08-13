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
import re

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
    Used for getting BeautifulSoup representation of webpage.
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
    date = event.attrib['date']
    meeting_num = int(event.attrib['meetingNumber'])

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

def get_senate_vote_details(senate_vote_url):
    """
    Returns a dict of all senate vote details of a bill from a given url.
    Args:
        senate_vote_url: the url to get senate vote details
    Returns: 
        dict: of all vote details of a bill in the following format

            sen_vote_details = {
                'C-30': # Bill name
                    [
                        {
                            'date': '...', # Vote details, as outline in data dictionary
                            'description': '...',
                            'yea': 10,
                            # Etc...
                        },
                        {
                            'date': '...', # Details for next vote with same bill name
                            'description': '...',
                            'yea': 9,
                            # Etc...
                        }
                    ],
                'C-12': [ {...}, {...} ], # Vote details for next bill, etc...
            }
    """
    sen_vote_details = {}

    # Get all bill votes urls
    sen_vote_urls = _get_all_senate_vote_urls(senate_vote_url)

    # Scrape vote details
    for (bill_name, bill_url) in sen_vote_urls:
        soup = get_soup(bill_url)
        vote_details = {}

        try:
            vote_details['date'] = _get_date_in_senate_vote_details(soup)
        except Exception as e:
            print(f'Cannot find date for {url}')

        vote_details['description'] = soup.select('div.vote-title-box > div.vote-web-title')[0].text.strip()
        vote_details['chamber'] = 'S'
        vote_details['votes'], votes_count = _get_votes_data_in_senate_vote_details(soup)

        for key in votes_count.keys():
            vote_details[key] = votes_count[key]

        passed_text = soup.select('div.summary-cell-tall')[0].text.strip()
        vote_details['passed'] = 1 if passed_text == 'Adopted' else 0

        try:
            sen_vote_details[bill_name].append(vote_details)
        except KeyError:
            sen_vote_details[bill_name] = [vote_details]

    return sen_vote_details

def _get_all_senate_vote_urls(senate_vote_url):
    """
    Returns a list of urls for senate votes on bills.
    Args:
        senate_vote_url: the url to a webpage that details senate votes on a page 
    Returns: list of tuples containing urls and corresponding bill name
    """
    sen_vote_urls = []
    sen_base_url = 'https://www.sencanada.ca'
    
    soup = get_soup(senate_vote_url)
    table_rows = soup.select('#votes-table > tbody')[0].find_all('tr')
    
    bill_name_idx = 2
    vote_url_idx = 1

    for tr in table_rows:
        table_cols = tr.find_all('td')
        try:
            bill_name = table_cols[bill_name_idx].find('a').text.strip()
            path = table_cols[vote_url_idx].find('a').get('href')
            sen_vote_urls.append((bill_name, sen_base_url + path))
        except Exception:
            pass

    return sen_vote_urls

def _get_date_in_senate_vote_details(soup):
    """
    Returns the date in datetime format from vote details.
    Args:
        soup: beautiful soup containing senate vote details 
    Returns: date of vote in datetime format
    """
    try:
        date_text = soup.select('div.vote-title-box')[0].find('div').text
        date_text = re.search(r'[A-Za-z]+, ([A-Za-z]+ [0-9]+, [0-9]+)', date_text).group(1)
        date = datetime.strptime(date_text, r'%B %d, %Y')
        date = datetime.strftime(date, r'%Y-%m-%d')
    except Exception as e:
        print(e)
        raise
    else:
        return date

def _get_votes_data_in_senate_vote_details(soup):
    """
    Returns participants vote data from vote details.
    Args:
        soup: beautiful soup containing senate vote details 
    Returns: participants vote data including {vote}_count and votes
    """
    counts = {
        'yea': 0,
        'nay': 0,
        'nv': 0,
        'absent': 0,
        'total': 0
    }
    votes = []
    table_rows = soup.select('#vote-details-table > tbody')[0].find_all('tr')
    
    for tr in table_rows:
        table_cols = tr.find_all('td')
        senator = table_cols[0].text.strip()
        province_territory = table_cols[2].text.strip()

        vote = _get_votetext_in_senate_vote_details(table_cols[3:])
        counts[vote] += 1

        search = {'province_territory': scraper_utils.get_prov_terr_abbrev(province_territory)}
        gov_id = _get_gov_id(senator, **search)
        
        vote_data = {
            'goverlytics_id': gov_id,
            'legislator': senator,
            'vote_text': vote
        }
        votes.append(vote_data)
    
    counts['total'] = counts['yea'] + counts['nay'] + counts['nv'] + counts['absent']

    return (votes, counts)

def _get_votetext_in_senate_vote_details(vote_cols):
    """
    Returns participant's votetext from vote details.
    Args:
        vote_cols: beautiful soup element containing votes statuses  
    Returns:
        str: participant's votetext
    """
    yea, nay, abstention = vote_cols
    
    # Participant's vote is indicated by having an data-order attribute of 'aaa'
    if yea.get('data-order') == 'aaa':
        return 'yea'
    if nay.get('data-order') == 'aaa':
        return 'nay'
    if abstention.get('data-order') == 'aaa':
        return 'absent'
    
    return 'nv'

def _get_gov_id(name, **search):
    """
    Returns legislator's goverlytics_id.
    Args:
        name: legislator's name
        search: extra query paramater to narrow the legislator's search
    Returns:
        str: goverlytics_id
    """
    last_name, remainder = name.split(', ')
    remainder = remainder.split(' ', 1)

    if len(remainder) == 1:
        first_name = remainder[0]
        gov_id = scraper_utils.get_legislator_id(name_last=last_name, name_first=first_name, **search)
    else:
        first_name, middle_name = remainder
        gov_id = scraper_utils.legislators_search_startswith('goverlytics_id','name_middle',
            middle_name, name_last=last_name, **search) 

    return gov_id

def get_mp_vote_details(bill_url):
    """
    Returns a list of all vote details of a bill from a given url.
    Args:
        bill_url: the url to get mp vote details
    Returns: list of all vote details of a bill
    """
    bill_vote_page_url = bill_url + '&View=5'
    soup = get_soup(bill_vote_page_url)

    bill_vote_details = [] 

    # Find all votes url
    try:
        list_of_votes = soup.find('ul', {'class': 'Votes'}).find_all('li')
    except Exception:
        # print(f'No vote exists for {bill_vote_page_url}')
        return bill_vote_details

    for hv_li in list_of_votes:
        vote_details = {}

        vote_details['description'] = hv_li.select('div.VoteInformation > div')[0].text.strip()
        vote_details['chamber'] = 'HOC'

        # Set other fields - date, passed, {vote}_count, votes
        hv_url = hv_li.select('a.VoteLink')[0].get('href') + '/xml'
        hv_url = hv_url.replace('//', 'https://')
        other_vote_details = _get_mp_other_vote_details(hv_url)
        for key in other_vote_details.keys():
            vote_details[key] = other_vote_details[key]

        bill_vote_details.append(vote_details)

    return bill_vote_details

def _get_mp_other_vote_details(xml_url):
    """
    Extracts vote details from a given bill XML URL.
    Args:
        xml_url: XML URL containing information about participants' vote details
    Returns: dict containing date, passed, {vote}_count, and votes
    """
    page = scraper_utils.request(xml_url)
    root = ET.fromstring(page.content)
    
    vote_details = {}
    yea_count, nay_count, abs_count, nv_count, total_count = 0, 0, 0, 0, 0
    votes = []

    # Collect vote data from each participants
    for vp in root.findall('VoteParticipant'):
        first_name = vp.find('PersonOfficialFirstName').text
        last_name = vp.find('PersonOfficialLastName').text
        province_territory = vp.find('ConstituencyProvinceTerritoryName').text
        source_id = vp.find('PersonId').text

        votetext = ''
        if vp.find('IsVoteYea').text == 'true':
            votetext = 'yea'
            yea_count += 1
        elif vp.find('IsVoteNay').text == 'true':
            votetext = 'nay'
            nay_count += 1
        elif vp.find('IsVotePaired').text == 'true':
            votetext = 'absent'
            abs_count += 1
        else:
            votetext = 'nv'
            nv_count += 1

        search = {
            'source_id': source_id,
            'province_territory': scraper_utils.get_prov_terr_abbrev(province_territory),
        }
        gov_id = _get_gov_id(f'{last_name}, {first_name}', **search)

        vote_data = {
            'goverlytics_id': gov_id,
            'legislator': f'{first_name} {last_name}',
            'vote_text': votetext
        }
        votes.append(vote_data)
    
    vote_details['yea'] = yea_count
    vote_details['nay'] = nay_count
    vote_details['nv'] = nv_count
    vote_details['absent'] = abs_count
    vote_details['total'] = yea_count + nay_count + nv_count + abs_count
    vote_details['votes'] = votes

    date_text = root.find('VoteParticipant').find('DecisionEventDateTime').text
    date = datetime.strptime(date_text, r'%Y-%m-%dT%H:%M:%S')
    date = datetime.strftime(date, r'%Y-%m-%d')
    vote_details['date'] = date

    passed_text = root.find('VoteParticipant').find('DecisionResultName').text
    passed = 1 if passed_text == 'Agreed To' else 0
    vote_details['passed'] = passed

    return vote_details

def scrape(xml_url):
    """
    Entry point for scraping. Bill data comes from an XML sheet that lists a set of 
    bills along with basic information. Since some datapoints are missing from the main
    XML sheet, additional data collection may occur, including data collection from an
    individual bill's information page.
    Args:
        xml_url: XML URL containing information about a list of bills
    Returns:
        List of rows containing bill information
    """
    page = scraper_utils.request(xml_url)
    root = ET.fromstring(page.content)

    current_session_number = get_current_session_number()

    bill_lst = []
    senate_vote_details = {}
    for bill in tqdm(root.findall('Bill'), "Bill scrapin'"):

        parl_session = bill.find('ParliamentSession')
        parl_number = parl_session.attrib["parliamentNumber"]

        if parl_number != current_session_number:
            continue

        row = scraper_utils.initialize_row()

        bill_id = bill.attrib['id']

        source_url = f'{base_url}/LegisInfo/BillDetails.aspx?Language=E&billId={bill_id}'

        introduced_date = datetime.strptime(
            bill.find('BillIntroducedDate').text, '%Y-%m-%dT%H:%M:%S')
        introduced_date = datetime.strftime(introduced_date, r'%Y-%m-%d')

        session = f'{parl_number}-{parl_session.attrib["sessionNumber"]}'

        bill_number = bill.find('BillNumber')
        bill_number = f'{bill_number.attrib["prefix"]}-{bill_number.attrib["number"]}'

        # Collect vote details
        try:
            if not senate_vote_details:
                senate_vote_url = f'https://sencanada.ca/en/in-the-chamber/votes/{session}'
                senate_vote_details = get_senate_vote_details(senate_vote_url)

            votes = get_mp_vote_details(source_url)
            votes += senate_vote_details.get(bill_number, [])
            row.votes = votes
        except Exception as e:
            print(e)
            print(f'There was a problem fetching votes from {source_url}')

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
        # pm_party_abbreviation = get_english_title(
        #     pm_details.find('PoliticalParty').find('abbreviation'))

        statute = bill.find('Statute')
        statute_year = int(statute.find('Year').text)
        statute_chapter = int(statute.find('Chapter').text)

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
        legislative_events = []
        for event in legislative_events_xml.findall('Event'):
            event_details = get_event_details(event)
            legislative_events.append({
                'date': event_details['date'],
                'description': event_details['status'],
                'action_by': event_details['chamber'],
                'committee': event_details['committee']
            })

        committees = []
        for le in legislative_events:
            if 'committee' in le and le['committee'] != '':
                committees.append(
                    {'chamber': le['action_by'], 'committee': le['committee']})

        row.committees = committees
        row.goverlytics_id = f'CAFED_{session}_{bill_number}'
        row.source_url = source_url
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
        # row.pm_party = party_switcher[pm_party] if pm_party_abbreviation in party_switcher else pm_party_abbreviation
        row.pm_party = party_switcher.get(pm_party, pm_party)
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

    xml_urls = xml_urls if len(xml_urls) > 0 else []

    xml_data = [scrape(xml_url) for xml_url in tqdm(xml_urls)]
    # scrape() returns a list so we need to coalesce data into one large list
    xml_data = [item for sublist in xml_data for item in sublist]

    if len(xml_data) > 0:
        scraper_utils.write_data(xml_data)

    print('Complete!')
