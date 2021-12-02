'''
Author: Avery Quan
Date: May 11, 2021

Notes:

- Scrape historical legislators by setting the historical field in get_urls() to true
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
from nameparser import HumanName
import pandas as pd
from multiprocessing.dummy import Pool
import traceback
from tqdm import tqdm
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


state_abbreviation = 'CO'
database_table_name = 'us_co_legislators'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

base_url = 'https://leg.colorado.gov'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_wiki_links(link, chamber):
    wikipedia_link = 'https://en.wikipedia.org'

    member_request = scraper_utils.request(link)
    member_soup = BeautifulSoup(member_request.content, 'html.parser')
    members = member_soup.find_all('table', class_='wikitable sortable')[1]
    members = members.find_all('tr')[1:]

    links = {}

    for member in members:

        elements = member.find_all('td')
        district = elements[0].text.strip()
        member_url = elements[1].find('a')['href']

        links[(chamber, district)] = wikipedia_link + member_url
    scraper_utils.crawl_delay(crawl_delay)
    return links

def get_urls(historical = True):
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''

    session_codes = []
    bill_search = 'https://leg.colorado.gov/bill-search?field_chamber=All&field_bill_type=All&field_sessions=66816&sort_bef_combine=field_bill_number%20ASC'
    page = scraper_utils.request(bill_search)
    soup = BeautifulSoup(page.content, 'html.parser')
    sessions = soup.find('select', id='edit-field-sessions').find_all('option')[1:]
    for session in sessions:
        session_codes.append(session['value'])

    if not historical:
        session_codes = [session_codes[0]]

    urls = {}

    for index, code in enumerate(session_codes):
        legislators_url = 'https://leg.colorado.gov/legislators' + '?session_id=' + code

        member_request = scraper_utils.request(legislators_url)
        member_soup = BeautifulSoup(member_request.content, 'html.parser')
        table = member_soup.find('table', id='legislators-overview-table')
        pandas = pd.read_html(str(table))[0]
        records = pandas.to_dict('records')

        scraper_utils.crawl_delay(crawl_delay)
        
        links = table.find_all('a')
        links = [base_url + link['href'] for link in links]
        

        for record, link in zip(records, links):
            chamber = {'Representative': 'House', 'Senator': 'Senate'}
            urls[(chamber[record['Title']], str(record['District']))] = {'url': link, 'records' : record, 'has_contacts': index == 0}

        # just adds the dictionaries together
        wiki_url = {**get_wiki_links('https://en.wikipedia.org/wiki/Colorado_Senate', 'Senate'), 
        **get_wiki_links('https://en.wikipedia.org/wiki/Colorado_House_of_Representatives', 'House')}
        
        info = []
        for key, path in urls.items():
            info.append([path, wiki_url[key]])
    return info


def scrape(info):
    try:
        print(info)
        wiki = info[1]
        url = info[0]

        # Send request to website
        page = scraper_utils.request(url['url'])
        page = BeautifulSoup(page.content, 'html.parser')

        row = scraper_utils.initialize_row()

        name = HumanName(url['records']['Name'])

        row.name_full = name.full_name
        row.name_first = name.first
        row.name_last = name.last
        row.name_middle = name.middle
        row.name_name_suffix = name.suffix

        # data from legislator list page
        row.party = url['records']['Party']
        row.party_id = scraper_utils.get_party_id(row.party)
        row.district = url['records']['District']
        if url['has_contacts']:
            row.phone_number = [{'office': 'Capitol', 'number': url['records']['Capitol Phone #']}]
            row.email = url['records']['Email']
        else :
            pass
        row.role = url[ 'records']['Title'].strip()
        if row.role == '':
            row.role = 'member'


        row.source_url = url['url']
        committees = page.find('div', class_='block-content').find_all('div', 'committee-assignment')
        for committee in committees:
            group = committee.find('h4', 'committee-link').text
            role = committee.find('div', 'committee-role').text.strip()
            if role == '':
                role = 'member'

            row.committees.append({'role': role, 'committee': group})

        address = page.find('div', class_='field field-name-field-contact-address field-type-addressfield field-label-above')
        div = address.find('div', class_='addressfield-container-inline locality-block country-US')
        span = address.find('div', class_='street-block')
        row.addresses.append({'location':'capitol', 'address': span.text + ' ' +  div.text})

        counties = page.find('div', class_='field field-name-field-counties field-type-entityreference field-label-above').find('div', 'field-items')
        row.areas_served = counties.text.strip().split('\n') 

        # Wiki fields below
        wiki = scraper_utils.scrape_wiki_bio(wiki)
        row.years_active = wiki['years_active']
        row.education = wiki['education']
        row.occupation = wiki['occupation']
        row.most_recent_term_id = wiki['most_recent_term_id']
        row.birthday = wiki['birthday']

        # Delay so we don't overburden web servers
        scraper_utils.crawl_delay(crawl_delay)
        return row
    except:
        traceback.print_exc()
        print(url)
    


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Speed things up using pool.
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database:
    scraper_utils.write_data(data)

    print('Complete!')
