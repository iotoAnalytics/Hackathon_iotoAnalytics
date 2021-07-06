'''
Author: Avery Quan
Date: May 26, 2021

Notes:
- Source url is not unique, have to modify scraper_utils to insert into database
    for this table I used full_name as the unique field
- Does not scrape historical
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


state_abbreviation = 'ID'
database_table_name = 'us_id_legislators'
scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)
base_url = 'https://legislature.idaho.gov'
crawl_delay = scraper_utils.get_crawl_delay(base_url)

wiki_urls = {}


def get_wiki_links(link, chamber):
    wikipedia_link = 'https://en.wikipedia.org'

    member_request = scraper_utils.request(link)
    scraper_utils.crawl_delay(crawl_delay)
    member_soup = BeautifulSoup(member_request.content, 'html.parser')
    members = member_soup.find_all('table', class_='wikitable sortable')[0]
    members = members.find_all('tr')[1:]
    links = {}

    if chamber == 'House':
        length = len(members)
        for i in range(0, length - 1, 2):
        
            district = members[i].find('th').text.strip()

            elements = members[i].find_all('td')
            member_url = elements[1].find('a')['href']
            seat = elements[0].text.strip()
            links[(chamber, district, seat)] = wikipedia_link + member_url

            elements = members[i + 1].find_all('td')
            member_url = elements[1].find('a')['href']
            seat = elements[0].text.strip()
            links[(chamber, district, seat)] = wikipedia_link + member_url
    
    else:
        for member in members:
            elements = member.find_all('td')
            district = elements[0].text.strip()
            member_url = elements[1].find('a')['href']

            links[(chamber, district, None)] = wikipedia_link + member_url


    return links

def get_urls():
    senate_url = 'https://legislature.idaho.gov/senate/membership/'
    house_url = 'https://legislature.idaho.gov/house/membership/'

    urls = [{'url': senate_url, 'chamber': 'Senate'}, {'url': house_url, 'chamber': 'House'} ]
    return urls


def scrape(info):
    
    url = info['url']
    chamber = info['chamber']

    page = scraper_utils.request(url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'html.parser')
    pages = soup.find_all('div', class_= 'wpb_column vc_column_container col-xs-mobile-fullwidth col-sm-4 text-left sm-text-left xs-text-left')
    rows = []
    for a in pages:
        try:
            row = scraper_utils.initialize_row()

            a_tag = a.find_all('a')
            for br in a.find_all("br"):
                br.replace_with(";;")
                
            fields = a.text.split(';;')
            if 'District' not in fields[2]:
                del fields[2]

            roles = {'Senate': 'Senator', 'House': 'Representative'}
            row.role = roles[chamber]
            name, party = fields[0].strip().rsplit(' ', 1)
            name = HumanName(name.replace('\xa0', '')) 
            row.name_full = name.full_name
            row.name_first = name.first
            row.name_last = name.last
            row.name_middle = name.middle
            row.name_name_suffix = name.suffix

            parties = {'(R)': 'Republican', '(D)': 'Democrat'}
            row.party = parties[party]
            row.party_id = scraper_utils.get_party_id(row.party)

            row.source_url = url

            row.email = a_tag[0].text
            row.district = a_tag[1].text.split(' ')[1]

            offset = 0
            if row.role == 'Senator':
                offset = -1  

            committees = a_tag[3:]
            for c in committees:
                next = str(c.next_sibling)
                if next != ' ':
                    role = next.split('–')[1].strip()
                else:
                    role = 'member'
                row.committees.append({'role': role, 'committee': c.text})

            addresses = fields[5 + offset].strip()
            if 'term' in addresses:
                addresses = fields[6 + offset].strip()
            row.addresses = {'location': 'home', 'address': addresses}

            try:
                label, phone_number_1 = fields[6 + offset].strip().split(' ', 1)
            except:
                label, phone_number_1 = [" ", " "]
                

            try:
                label2, phone_number_2 = fields[7 + offset].strip().split(' ', 1)
            except:
                traceback.print_exc()   
                print(url)
                print(row)
                label2, phone_number_2 = [" ", " "]

            if 'Home' not in label or 'Statehouse' not in label:
                try:
                    label, phone_number_1 = fields[7 + offset].strip().split(' ', 1)
                except:
                    pass
                try:
                    label2, phone_number_2 = fields[8 + offset].strip().split(' ', 1)
                except:
                    pass

            if 'Bus' in phone_number_2:
                phone_number_2 = fields[8 + offset].strip().split(' ')
            row.phone_numbers = [{"office": label.lower(), "number": phone_number_1.replace('(Session Only)', '')}, 
                            {"office": label2.lower(), "number": phone_number_2.replace('(Session Only)', '')}]

            

            if 'FAX' in fields[8 + offset] or 'Statehouse'  in fields[8 + offset] or 'Bus' in fields[8 + offset]:
                if 'FAX' in fields[9 + offset] or 'Statehouse'  in fields[9 + offset] or 'Bus' in fields[9 + offset]:
                    row.occupation = [fields[10 + offset].strip()]
                else:
                    row.occupation = [fields[9 + offset].strip()]
            else:
                row.occupation = [fields[8 + offset].strip()]

            # Wiki fields below

            try:   
                seat = 'B' if 'B' in fields[3] else 'A'
                seat = None if row.role == 'Senator' else seat
                wiki_url = wiki_urls[(chamber, row.district, seat)]

                wiki = scraper_utils.scrape_wiki_bio(wiki_url)
                row.years_active = wiki['years_active']
                row.education = wiki['education']
                if len(wiki['occupation']) != 0 and len(row.occupation) == 0:
                    row.occupation = wiki['occupation']
                row.most_recent_term_id = wiki['most_recent_term_id']
                row.birthday = wiki['birthday']
            except:
                traceback.print_exc()


            # Delay so we don't overburden web servers
            scraper_utils.crawl_delay(crawl_delay)
            rows.append(row)
        except:
            traceback.print_exc()   
            print(url)
            print(row)
    return rows


if __name__ == '__main__':
    senate_wiki = get_wiki_links('https://en.wikipedia.org/wiki/Idaho_Senate', 'Senate')
    house_wiki = get_wiki_links('https://en.wikipedia.org/wiki/Idaho_House_of_Representatives', 'House')
    wiki_urls = senate_wiki | house_wiki
    
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Speed things up using pool.
    with Pool() as pool:
        data = pool.map(scrape, urls)

    data = data[0] + data[1]

    # Once we collect the data, we'll write it to the database:
    scraper_utils.write_data(data)

    print('Complete!')
