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

from regex import Regex

p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
from nameparser import HumanName
import pandas as pd
from multiprocessing.dummy import Pool
from urllib.request import urlopen as uReq
import numpy as np
import traceback
from tqdm import tqdm
import ssl
import re
ssl._create_default_https_context = ssl._create_unverified_context


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


def find_individual_wiki(wiki_page_link):
    bio_lnks = []
    uClient = uReq(wiki_page_link)
    page_html = uClient.read()
    uClient.close()

    page_soup = BeautifulSoup(page_html, "lxml")
    tables = page_soup.findAll("table")
    rows = tables[3].findAll("tr")

    for person in rows[1:]:
        info = person.findAll("td")
        try:
            biolink = info[1].a["href"]

            bio_lnks.append(biolink)

        except Exception:
            pass
    scraper_utils.crawl_delay(crawl_delay)
    return bio_lnks


def get_urls():
    senate_url = 'https://legislature.idaho.gov/senate/membership/'
    house_url = 'https://legislature.idaho.gov/house/membership/'

    urls = [{'url': senate_url, 'chamber': 'Senate'}, {'url': house_url, 'chamber': 'House'} ]
    return urls


def get_wiki_url(row):

    wikipage_reps = "https://ballotpedia.org/Idaho_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Idaho_State_Senate"

    if row.role == "Representative":
        try:
            uClient = uReq(wikipage_reps)
            page_html = uClient.read()
            uClient.close()

            page_soup = BeautifulSoup(page_html, "lxml")
            tables = page_soup.findAll("table")
            rows = tables[3].findAll("tr")

            for person in rows[1:]:
                tds = person.findAll("td")
                name_td = tds[1]
                name = name_td.text
                name = name.replace('\n', '')
                party = tds[2].text
                party = party.strip()
                party = party.replace('\n', '')
                if party == "Democratic":
                    party = "Democrat"

                try:
                    if row.party == party and row.name_last in name.strip() and name.strip().split(" ")[0] in row.name_first:
                        row.wiki_url = name_td.a['href']
                        break
                except:
                        pass
                if not row.wiki_url:
                    for person in rows[1:]:
                        tds = person.findAll("td")
                        name_td = tds[1]
                        name = name_td.text
                        name = name.replace('\n', '')
                        party = tds[2].text
                        party = party.strip()

                        if party == "Democratic":
                            party = "Democrat"

                        if row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                            row.wiki_url = name_td.a['href']
                            break
                        elif row.party == party and row.name_last in name.strip().split()[-1]:
                            row.wiki_url = name_td.a['href']
                            break
        except Exception as e:
            print(e)
    if row.role == "Senator":

        try:
            uClient = uReq(wikipage_senate)
            page_html = uClient.read()
            uClient.close()

            page_soup = BeautifulSoup(page_html, "lxml")
            tables = page_soup.findAll("table")
            rows = tables[3].findAll("tr")

            for person in rows[1:]:
                tds = person.findAll("td")
                name_td = tds[1]
                name = name_td.text
                name = name.replace('\n', '')
                party = tds[2].text
                party = party.strip()

                if party == "Democratic":
                    party = "Democrat"

                try:
                    if row.party == party and row.name_last in name.strip().split()[-1] and name.strip().split(" ")[0] in row.name_first:
                        row.wiki_url = name_td.a['href']
                        break
                except:
                    pass
            if not row.wiki_url:
                for person in rows[1:]:
                    tds = person.findAll("td")
                    name_td = tds[1]
                    name = name_td.text
                    name = name.replace('\n', '')
                    party = tds[2].text
                    party = party.strip()

                    if party == "Democratic":
                        party = "Democrat"

                    if row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
                    elif row.party == party and row.name_last in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
        except Exception as e:
            print(e)
            pass

def scrape(info):
    
    url = info['url']
    print(url)
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

            #row.source_url = url

            row.email = a_tag[0].text
            row.source_url = row.email
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

            address_list = []
            addresses = fields[5 + offset].strip()
            if 'term' in addresses:
                addresses = fields[6 + offset].strip()
                address_list.append({'location': 'home', 'address': addresses})
            row.addresses = address_list

            try:
                label, phone_number_1 = fields[6 + offset].strip().split(' ', 1)
            except:
                label, phone_number_1 = [" ", "000-000-0000"]
                

            try:
                label2, phone_number_2 = fields[7 + offset].strip().split(' ', 1)
            except:
                traceback.print_exc()   
                print(url)
                print(row)
                label2, phone_number_2 = [" ", "000-000-0000"]

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
            try:
                phone_number_1 = phone_number_1.replace(' (Session Only)', '').replace('(', '').replace(')', '').replace(' ', '-')
            except:
                pass
            try:
                phone_number_2 = phone_number_2.replace(' (Session Only)', '').replace('(', '').replace(')', '').replace(' ', '-')
            except:
                pass
            phone_numbers=[]
            phone_numbers.append({"office": label.lower(), "number": phone_number_1})
            if not any(c.isalpha() for c in phone_number_2):
                phone_numbers.append({"office": label2.lower(), "number": phone_number_2})

            row.phone_numbers = phone_numbers

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

            get_wiki_url(row)
            gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
            if not gender:
                gender = 'O'
            row.gender = gender

            # Delay so we don't overburden web servers
            scraper_utils.crawl_delay(crawl_delay)
            rows.append(row)
        except:
            traceback.print_exc()   
            print(url)

    return rows


if __name__ == '__main__':
    senate_wiki = get_wiki_links('https://en.wikipedia.org/wiki/Idaho_Senate', 'Senate')
    house_wiki = get_wiki_links('https://en.wikipedia.org/wiki/Idaho_House_of_Representatives', 'House')
    wiki_urls = {**senate_wiki, **house_wiki}
    
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Speed things up using pool.
    with Pool() as pool:
        data = pool.map(scrape, urls)

    data = data[0] + data[1]

    leg_df = pd.DataFrame(data)

    # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/Idaho_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Idaho_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['name_last', 'wiki_url']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_last", 'wiki_url'])

    isna = big_df['education'].isna()
    big_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df['wiki_url'] = big_df['wiki_url'].replace({np.nan: None})

    big_list_of_dicts = big_df.to_dict('records')
    print(big_list_of_dicts)
    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
