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
    member_soup = BeautifulSoup(member_request.content, 'html.parser')
    members = member_soup.find_all('table', class_='wikitable sortable')[0]
    members = members.find_all('tr')[1:]

    links = {}

    for member in members:

        elements = member.find_all('td')
        district = elements[0].text.strip()
        member_url = elements[1].find('a')['href']

        links[(chamber, district)] = wikipedia_link + member_url
    scraper_utils.crawl_delay(crawl_delay)
    return links

def get_urls():
    senate_url = 'https://legislature.idaho.gov/senate/membership/'
    house_url = 'https://legislature.idaho.gov/house/membership/'

    urls = [{'url': senate_url, 'chamber': 'Senate'}, {'url': house_url, 'chamber': 'House'} ]
    return urls


def scrape(info):
    try:
        url = info['url']
        chamber = info['chamber']

        page = scraper_utils.request(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        pages = soup.find_all('div', class_= 'wpb_column vc_column_container col-xs-mobile-fullwidth col-sm-4 text-left sm-text-left xs-text-left')
        rows = []
        for a in pages:
            row = scraper_utils.initialize_row()

            a_tag = a.find_all('a')
            for br in a.find_all("br"):
                br.replace_with(";;")
                
            fields = a.text.split(';;')
            if 'District' not in fields[2]:
                del fields[2]
            print(fields)

            name, role = fields[0].strip().rsplit(' ', 1)
            name = HumanName(name.replace('\xa0', '')) 
            row.name_full = name.full_name
            row.name_first = name.first
            row.name_last = name.last
            row.name_middle = name.middle
            row.name_name_suffix = name.suffix

            roles = {'(R)': 'Representative', '(S)': 'Senator'}
            row.role = roles[role]

            row.email = a_tag[0]
            row.district = a_tag[1].text.split(' ')[1]

            committees = a_tag[3:]
            for c in committees:
                row.committees.append({'role': 'member', 'committee': c.text})

            addresses = fields[5].strip()
            row.addresses = {'location': 'home', 'address': addresses}

            label, phone_number_1 = fields[6].strip().split(' ', 1)

            phone_number_2 = fields[7].strip().split(' ')

            if 'Bus' in phone_number_2:
                phone_number_2 = fields[8].strip().split(' ')
            label2 = phone_number_2[0]
            phone_number_2 = ' '.join(phone_number_2[1:-2])
            row.phone_number = [{"office": label.lower(), "number": phone_number_1}, 
                            {"office": label2.lower(), "number": phone_number_2}]

            if 'Committees' not in fields[8]:
                if 'Fax' in fields[8]:
                    row.occupation = fields[9].strip()
                else:
                    row.occupation = fields[8].strip()

            # Wiki fields below

            try:   
                wiki_url = wiki_urls[(chamber, row.district)]

                wiki = scraper_utils.scrape_wiki_bio(wiki_url)
                row.years_active = wiki['years_active']
                row.education = wiki['education']
                row.occupation = wiki['occupation']
                row.most_recent_term_id = wiki['most_recent_term_id']
                row.birthday = wiki['birthday']
            except:
                traceback.print_exc()


            # Delay so we don't overburden web servers
            # scraper_utils.crawl_delay(crawl_delay)
            rows.append(row)
    except:
        traceback.print_exc()   
        print(url)
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

    data = data[0][0] + data[1][0]
    print(data)

    # Once we collect the data, we'll write it to the database:
    scraper_utils.write_data(data)

    print('Complete!')
