import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))
sys.setrecursionlimit(5000)

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
from pprint import pprint
from nameparser import HumanName
import pandas as pd
import re
import boto3
import time
from urllib.request import urlopen as uReq
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

senate_url = 'https://www.scstatehouse.gov/member.php?chamber=S'
house_url = 'https://www.scstatehouse.gov/member.php?chamber=H'
base_url = 'https://www.scstatehouse.gov'

wiki_link = 'https://en.wikipedia.org/wiki/South_Carolina_General_Assembly'
base_wiki = 'https://en.wikipedia.org/'

state_abbreviation = 'SC'
database_table_name = 'us_sc_legislators'

#Put most recent year here
present = 2021

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


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

def match_wiki_link(wiki_url, dict_item_list):
    url_request = requests.get(wiki_url)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    for dict_item in dict_item_list:
        hn = HumanName(dict_item['name'])
        if dict_item['role'] == 'Senator':
            legislator_names = url_soup.find('div', {'aria-labelledby': 'Members_of_the_South_Carolina_Senate'})
        elif dict_item['role'] == 'Representative':
            legislator_names = url_soup.find('div',
                                             {
                                                 'aria-labelledby': 'Members_of_the_South_Carolina_House_of_Representatives'})
        for legislator_name in legislator_names.find('td', {'class': 'navbox-list navbox-odd'}).find_all('li'):
            legislator_html = legislator_name.find('a')
            if hn.first in legislator_html.get('title') and hn.last in legislator_html.get('title'):
                if 'redlink' not in legislator_html.get('href'):
                    dict_item['wiki_link'] = base_wiki + legislator_html.get('href')
                    print(f'done finding wiki link for {hn.first}')
    scraper_utils.crawl_delay(crawl_delay)
    return dict_item_list


def edit_years_range(years_range_lst):
    first = years_range_lst[0].strip()
    if ',' in first:
        first = first.split(',')[1]
    elif ' ' in first:
        first = first.split(' ')[1]
    first = int(first.strip())
    if years_range_lst[1].strip() == 'Present':
        years_active = list(range(first, present)) + [present]
    else:
        last = years_range_lst[1].strip()
        if ',' in last:
            last = last.split(',')[1]
        elif ' ' in last:
            last = last.split(' ')[1]
        last = int(last.strip())
        years_active = list(range(first, last)) + [last]
    return years_active


def get_legislator_links(url):
    legislator_list = []
    role = url.split('chamber=')[1]
    if role == 'S':
        role = 'Senator'
    elif role == 'H':
        role = 'Representative'
    url_request = requests.get(url)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_table = url_soup.find('div', {'id': 'contentsection'}).find_all('div', {'class': 'memberOutline'})
    for item in url_table:
        district = item.find('div', {'class': 'district'}).text.replace('District', '').strip()
        name = item.find('a', {'class': 'membername'}).text.replace('Senator', '').replace('Representative', '').strip()
        link = base_url + item.find('a', {'class': 'membername'}).get('href')
        source_id = link.split('code=')[1]
        if '(R)' in item.text:
            party = 'Republican'
        elif '(D)' in item.text:
            party = 'Democrat'
        address = item.find('div', {'id': 'address'}).text
        legislator_list.append({
            'name': name,
            'source_id': source_id,
            'role': role,
            'district': district,
            'party': party,
            'address': address,
            'link': link
        })
    scraper_utils.crawl_delay(crawl_delay)
    return legislator_list


def get_wiki_url(row):

    wikipage_reps = "https://ballotpedia.org/South_Carolina_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/South_Carolina_State_Senate"

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
                party = party.replace('\n', '').replace('.', '')
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


def scrape_info(dict_item):
    row = scraper_utils.initialize_row()

    link = dict_item['link']
    row.source_url = link

    hn = HumanName(dict_item['name'])
    row.name_full = dict_item['name']
    row.name_last = hn.last.title()
    row.name_first = hn.first.title()
    row.name_middle = hn.middle.title()
    row.name_suffix = hn.suffix

    row.source_id = dict_item['source_id']
    row.role = dict_item['role']
    row.district = dict_item['district']
    row.party = dict_item['party']
    row.party_id = scraper_utils.get_party_id(dict_item['party'])

    url_request = requests.get(link)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    address_html = url_soup.find('div', {'style': 'float: left; width: 225px; margin: 10px 5px 0 20px; padding: 0;'})
    office = address_html.find('h2').text.replace('\n', '')
    address = address_html.find('p').text.replace('\n', '')
    addresses = [{
        'location': office,
        'address': address
    }, {
        'location': 'Home Address',
        'address': dict_item['address']
    }]
    row.addresses = addresses

    numbers = address_html.find_all('p', {'style': 'font-size: 13px; margin: 0 0 0 0; padding: 0;'})
    for item in numbers:
        if 'Business Phone' in item.text:
            number = item.text.split('Phone')[1].strip()
            number = number.replace('(', '').replace(')', '').replace(' ', '-')
            print(number)
            phone = [{
                'office': office,
                'number': number
            }]
    row.phone_numbers = phone

    table_html = url_soup.find('table', {'style': 'margin: 0 10px 10px 10px; padding: 0; width: 100%;'}).find_all(
        'td', {'width': '50%', 'valign': 'top'})[1]

    coms_lst = []
    coms_html = table_html.find('ul').find_all('li')
    for item in coms_html:
        coms = item.text.split(',')
        if len(coms) > 1 and coms[0] != 'Fish':
            coms_lst.append({
                'role': coms[1],
                'committee': coms[0]
            })
        elif len(coms) > 1 and coms[0] == 'Fish':
            coms_lst.append({
                'role': 'member',
                'committee': ''.join(coms)
            })
        elif len(coms) == 1:
            coms_lst.append({
                'role': 'member',
                'committee': coms[0]
            })
    row.committees = coms_lst

    try:
        service_html = table_html.find_all('ul')[-1].find_all('li')
        years_active = []
        for item in service_html:
            text = item.text
            if 'Senate' in text or 'Representative' in text:
                years_active += edit_years_range(text.split(',', 1)[1].split('-'))
        years_active = sorted(list(dict.fromkeys(years_active)))
        row.years_active = years_active

        row.most_recent_term_id = str(years_active[-1])
    except IndexError:
        pass

    # for scraping wikipedia, need to check if wiki_link is a valid key in the dictionary
    if wiki_link in dict_item:
        wiki_info = scraper_utils.scrape_wiki_bio(dict_item['wiki_link'])
        row.birthday = wiki_info['birthday']
        row.occupation = wiki_info['occupation']
        row.education = wiki_info['education']
        row.years_active = wiki_info['years_active']
        row.most_recent_term_id = wiki_info['most_recent_term_id']
    get_wiki_url(row)
    gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
    if not gender:
        gender = 'O'
    row.gender = gender



    scraper_utils.crawl_delay(crawl_delay)
    print(f'done row for {hn.first} {hn.last}')
    # print(row)
    return row


if __name__ == '__main__':
    legis_dicts = get_legislator_links(senate_url) + get_legislator_links(house_url)
    legis_dicts = match_wiki_link(wiki_link, legis_dicts)

    # Was running into connection problems with the south carolina servers, and the crawl delays didnt seem to help too
    # much, so splitting the scraping process into 2 with a resting time in the middle seemed to help
    # with Pool(processes=5) as pool:
    #     data = pool.map(scrape_info, legis_dicts[0:10])

    data = []
    with Pool(processes=5) as pool:
        data += pool.map(scrape_info, legis_dicts[:len(legis_dicts) // 2])

    time.sleep(3)
    print('resting...')

    with Pool(processes=5) as pool:
        data += pool.map(scrape_info, legis_dicts[len(legis_dicts) // 2:])


    leg_df = pd.DataFrame(data)

    # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/South_Carolina_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/South_Carolina_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['name_last', 'wiki_url']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                          on=["name_last", 'wiki_url'])

    big_df.drop(big_df.index[big_df['wiki_url'] == ''], inplace=True)

    big_list_of_dicts = big_df.to_dict('records')


    scraper_utils.write_data(big_list_of_dicts)
    print('Complete!')
