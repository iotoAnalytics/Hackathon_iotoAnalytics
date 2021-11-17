'''
This template is meant to serve as a general outline, and will not necessarily work for
all collectors. Feel free to modify the script as necessary.
'''
import sys
import os
from pathlib import Path
from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from nameparser import HumanName
from database import Database
from pprint import pprint
import re
import boto3
import time
from urllib.request import urlopen as uReq
import pandas as pd
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

state_abbreviation = 'OH'
database_table_name = 'us_oh_legislators'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

house_url = 'https://www.legislature.ohio.gov/legislators/house-directory'
senate_url = 'https://www.legislature.ohio.gov/legislators/senate-directory'

base_url = 'https://www.legislature.ohio.gov'
senate_committee = 'https://ohiosenate.gov/committees'
base_sen = 'http://ohiosenate.gov/'

wiki_link = 'https://en.wikipedia.org/wiki/Ohio_General_Assembly'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_legislator_wiki_link(name_first, name_last, role):
    response =scraper_utils.request(wiki_link)
    #response = requests.get(wiki_link)
    soup = BeautifulSoup(response.content, 'lxml')
    if role == 'Senator':
        legislator_lst = soup.find('div', {'aria-labelledby': 'Current_members_of_the_Ohio_Senate'}).find_all('li')
    elif role == 'Representative':
        legislator_lst = soup.find('div', {'aria-labelledby': 'Members_of_the_Ohio_House_of_Representatives'}).find_all(
            'li')
    for legislator in legislator_lst:
        try:
            try:
                legislator_wiki_title = legislator.find('a').get('title')
            except:
                pass
            if name_first in legislator_wiki_title and name_last in legislator_wiki_title:
                return legislator.find('a').get('href')
        except TypeError:
            pass
    return None


def get_rep_coms(link):
    split = 'http://ohiohouse.gov/'
    name = link.split(split)[1]
    com_list_url = f'{split}members/{name}/committees'
    response = scraper_utils.request(com_list_url)
    #response = requests.get(com_list_url)
    soup = BeautifulSoup(response.content, 'lxml')
    try:
        com_link_html = soup.find('div', {'class': 'gray-block'}).find_all('a')
        com_links = [split[:-1] + x.get('href') for x in com_link_html]
        coms_lst = []
        for link in com_links:
            response = scraper_utils.request(link)
            #response = requests.get(link)
            soup = BeautifulSoup(response.content, 'lxml')
            div_lst = soup.find('div', {'class': 'gray-block'}).find_all('div')[1:]
            for item in div_lst:
                try:
                    compare_name = item.find('a').get('href')
                    if name in compare_name:
                        try:
                            position = item.find('div', {'class': 'committee-member-position'}).text
                        except:
                            position = 'member'
                        coms_lst.append({
                            'committee': ' '.join(link.split('/')[-1].split('-')),
                            'role': position
                        })
                except AttributeError:
                    pass
            scraper_utils.crawl_delay(crawl_delay)

        scraper_utils.crawl_delay(crawl_delay)
        return coms_lst
    except AttributeError:
        return []


def get_sen_coms(url):
    response = scraper_utils.request(url)
    #response = requests.get(url)
    soup = BeautifulSoup(response.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    com_lst = soup.find('div', {'class': 'committeeGrid'}).find_all('a')
    com_lst = [base_sen + x.get('href') for x in com_lst]
    com_dict = {}
    for link in com_lst:
        com = link.split('/')[-1]

        com_soup = BeautifulSoup(scraper_utils.request(link).content, 'lxml')
        scraper_utils.crawl_delay(crawl_delay)
        legis_lst = com_soup.find('div', {'class': 'portraitGroupModule'}).find_all('div', {'class':'profileInfo'})
        for item in legis_lst:
            try:
                name = item.find('div', {'class': 'profileName'}).text
                position = item.find('div', {'class': 'profilePosition'})
                link = item.find('div', {'class': 'profileName'}).find('a').get('href')
                link = base_sen + link.replace('../', '')
                current_com = {'role': 'member' if not position else position.text, 'committee': com}
                if link not in com_dict:
                    com_dict[link] = {'name': name,
                                      'committees': [current_com]}
                else:
                    com_dict[link]['committees'].append(current_com)
            except:
                pass
    return com_dict


def get_legislator_links(url):
    print(url)
    los = []
    request = scraper_utils.request(url)
    #request = requests.get(url)
    soup = BeautifulSoup(request.content, 'lxml')
    legislator_table = soup.find('div', {'class': 'mediaGrid mediaGridDirectory'}).find_all('a')
    role = 'Representative' if 'house-directory' in url else 'Senator'

    for legislator in legislator_table:
        link = legislator.get('href')
        name = legislator.find('div', {'class': 'mediaCaptionTitle'}).text
        hn = HumanName(name)
        info = legislator.find('div', {'class': 'mediaCaptionSubtitle'}).text.split('|')
        district = info[0].replace('District', '').strip()
        party = 'Republican' if info[1].strip() == 'R' else 'Democrat'
        los.append({
            'link': link,
            'name_full': name,
            'name_first': hn.first,
            'name_last': hn.last,
            'name_middle': hn.middle,
            'name_suffix': hn.suffix,
            'party': party,
            'role': role,
            'district': district
        })

    scraper_utils.crawl_delay(crawl_delay)

    return los

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


def get_wiki_url(row):
    wikipage_reps = "https://ballotpedia.org/Ohio_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Ohio_State_Senate"

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

def scrape(legis_dict):
    # Send request to website
    url = legis_dict['link']
    row = scraper_utils.initialize_row()

    row.source_url = url
    row.name_full = legis_dict['name_full'].title()
    row.name_first = legis_dict['name_first'].title()
    row.name_last = legis_dict['name_last'].title()
    row.name_middle = legis_dict['name_middle']
    row.name_suffix = legis_dict['name_suffix']

    row.district = legis_dict['district']
    role = legis_dict['role']
    row.role = role

    party = legis_dict['party']
    row.party = party
    row.party_id = scraper_utils.get_party_id(party)

    response = scraper_utils.request(url)
    #response = requests.get(url)
    soup = BeautifulSoup(response.content, 'lxml')
    if role == 'Representative':
        page_info = list(filter(None, soup.find_all('div', {'class': 'member-info-bar-module'})[-1].text.split('\n')))
        phone = page_info[1].strip()

        phone = phone.replace('(', '').replace(')', '').replace(' ', '-').replace('Phone:-','')
        print(phone)
        row.addresses = [{
            'location': 'district office',
            'address': page_info[0]
        }]
        row.phone_numbers = [{
            'office': 'district office',
            'number': phone
        }]
    elif role == 'Senator':
        page_info = soup.find('div', {'class': 'generalInfoModule'})
        address = page_info.find('div', {'class': 'address'}).text.replace('\n', '').strip()
        row.addresses = [{
            'location': 'district office',
            'address': address
        }]

        phone = page_info.find('div', {'class': 'phone'}).text.replace('\n', '').strip()
        phone = phone.replace('(','').replace(')','').replace(' ','-')

        row.phone_numbers = [{
            'office': 'district office',
            'number': phone
        }]

    row.committees = get_rep_coms(url) if role == 'Representative' else legis_dict['committees']

    legis_wiki_link = get_legislator_wiki_link(legis_dict['name_first'], legis_dict['name_last'], role)
    if legis_wiki_link:
        wiki_info = scraper_utils.scrape_wiki_bio(legis_wiki_link)
        row.birthday = wiki_info['birthday']
        row.education = wiki_info['education']
        row.occupation = wiki_info['occupation']
        row.years_active = wiki_info['years_active']
        row.most_recent_term_id = wiki_info['most_recent_term_id']
    get_wiki_url(row)
    gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
    if not gender:
        gender = 'O'
    row.gender = gender
    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)
    print(row)

    return row


if __name__ == '__main__':
    house_dicts = get_legislator_links(house_url)
    sen_dicts = get_legislator_links(senate_url)
    sen_coms = get_sen_coms(senate_committee)

    for senator in sen_dicts:
        if senator['link'] in sen_coms:
            senator['committees'] = sen_coms[senator['link']]['committees']

    legis_dicts = sen_dicts + house_dicts

    print('done getting dicts')

    with Pool() as pool:
        data = pool.map(scrape, legis_dicts)

    leg_df = pd.DataFrame(data)

    # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/Ohio_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Ohio_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['name_last', 'wiki_url']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                        on=["name_last", 'wiki_url'])


    print('Scraping complete')

    big_df.drop(big_df.index[big_df['wiki_url'] == ''], inplace=True)

    big_list_of_dicts = big_df.to_dict('records')

    print('Writing data to database...')
    scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
