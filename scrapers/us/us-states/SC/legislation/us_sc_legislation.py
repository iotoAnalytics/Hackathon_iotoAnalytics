import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))
from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
import time
from multiprocessing import Pool
from itertools import chain
import pandas as pd
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3

state_abbreviation = 'SC'
database_table_name = 'us_sc_legislation'
legislator_table_name = 'us_sc_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

senate_bills = 'https://www.scstatehouse.gov/sessphp/sintros.php'
house_bills = 'https://www.scstatehouse.gov/sessphp/hintros.php'
base_url = 'https://www.scstatehouse.gov'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


# Splits list 'a' into 'n' parts
def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


def get_daily_bills(bill_list_link_list):
    return_links = []
    for bill_list_link in bill_list_link_list:
        url_requests = requests.get(bill_list_link)
        url_soup = BeautifulSoup(url_requests.content, 'lxml')
        url_links = url_soup.find('div', {'style': 'margin-left: 60px'}).find_all('a')
        for link in url_links:
            return_links.append(base_url + link.get('href'))
    scraper_utils.crawl_delay(crawl_delay)
    return return_links


def get_bill_links(daily_bills_page):
    return_links = []
    url_requests = requests.get(daily_bills_page)
    url_soup = BeautifulSoup(url_requests.content, 'lxml')
    url_links = url_soup.find_all('a')
    for link in url_links:
        if 'billsearch.php?' in link.get('href'):
            return_links.append(base_url + link.get('href'))
    # scraper_utils.crawl_delay(crawl_delay)
    return return_links


def get_vote_info(vote_link):
    request = requests.get(vote_link)
    soup = BeautifulSoup(request.content, 'lxml')
    info_dict = pd.read_html(str(soup.find('table')))[0].to_dict('records')[1:]
    vote_list = []
    for item in info_dict:
        date = item['Date/Time'].split('\xa0')[0]
        description = item['Motion'].replace('\xa0', ' ')
        yea = item['Yeas']
        nay = item['Nays']
        nv = item['N/V']
        absent = item['Exc.Abs.']
        total = item['Total']
        if item['Result'] == 'Passed':
            passed = 1
        else:
            passed = 0
        vote_list.append({
            'date': date,
            'description': description,
            'passed': passed,
            'yea': yea,
            'nay': nay,
            'nv': nv,
            'absent': absent,
            'total': total
        })
    return vote_list


def get_actions(bill_link):
    request = requests.get(bill_link)
    soup = BeautifulSoup(request.content, 'lxml')
    td_list = soup.find('table').find_all('td')
    action_list = []
    for item in td_list:
        if td_list.index(item) % 3 == 0:
            #             date = item.text.split('-')
            action_item = {
                'date': item.text
            }
        if td_list.index(item) % 3 == 1:
            action_item['action_by'] = item.text
        if td_list.index(item) % 3 == 2:
            action_item['description'] = item.text
            action_list.append(action_item)
    current_action = action_list[-1]
    return [current_action, action_list]


def edit_sponsors(sponsor_text):
    legislator_type = ''
    if 'Senator' in sponsor_text or 'Senators' in sponsor_text:
        if 'Senators' in sponsor_text:
            legislator_type = 'Senators'
        else:
            legislator_type = 'Senator'
    elif 'Rep' in sponsor_text or 'Reps' in sponsor_text:
        if 'Reps.' in sponsor_text:
            legislator_type = 'Reps.'
        else:
            legislator_type = 'Rep.'
    sponsor_text = sponsor_text.replace(legislator_type, ' ').strip()
    sponsor_list = sponsor_text.split(',')
    for el in sponsor_list:
        if ' and' in el:
            and_legislators = [x.strip() for x in el.split(' and')]
            sponsor_list = sponsor_list[:-1] + and_legislators
    return [x.title().strip() for x in sponsor_list]


def scrape(url):
    row = scraper_utils.initialize_row()

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:

    row.source_url = url
    session = re.search(r'session=[0-9]+', url).group().replace('session=', '')
    row.session = session

    request = requests.get(url)
    soup = BeautifulSoup(request.content, 'lxml')
    bill_name_string = soup.find('span', {'style': 'font-weight:bold;'}).text.split(',')[0]
    bill_name_split = bill_name_string.split()
    if '*' in bill_name_string:
        bill_name = bill_name_split[0].replace('*', '_')
        bill_type = ' '.join(bill_name_split[1:])
        row.bill_name = bill_name
        row.bill_type = bill_type
    else:
        bill_name = '_'.join(bill_name_split[:2])
        bill_type = ' '.join(bill_name_split[2:])
        row.bill_name = bill_name
        row.bill_type = bill_type

    if bill_name[0] == 'S':
        chamber_origin = 'Senate'
        row.chamber_origin = chamber_origin
        role = 'Senator'
    elif bill_name[0] == 'H':
        chamber_origin = 'House'
        row.chamber_origin = chamber_origin
        role = 'Representative'

    td_list = soup.find('table').find_all('td')
    action_list = []
    for item in td_list:
        if td_list.index(item) % 3 == 0:
            action_item = {
                'date': item.text
            }
        if td_list.index(item) % 3 == 1:
            action_item['action_by'] = item.text
        if td_list.index(item) % 3 == 2:
            action_item['description'] = item.text
            action_list.append(action_item)
    current_action = action_list[-1]
    row.actions = action_list
    row.current_status = current_action['description']

    display_links = soup.find_all('a', {'class': 'nodisplay'})
    full_text_link = display_links[1].get('href')
    full_text_link = base_url + full_text_link

    try:
        vote_link = base_url + display_links[2].get('href')
        row.votes = get_vote_info(vote_link)
    except:
        pass

    request1 = requests.get(full_text_link)
    soup1 = BeautifulSoup(request1.content, 'lxml')
    text = soup1.text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').strip()
    text_lst = max([x for x in text.split(')') if x], key=len).strip()

    row.bill_text = text_lst
    summary_split = soup.find('div', {'class': 'bill-list-item'}).text.split('\xa0')
    row.bill_summary = [x for x in summary_split if x][1].split('View full text')[0]

    p_list = soup1.find_all('p')
    sponsors = []
    # MIGHT NEED TO GET THE SPONSOR INFO FROM THE WEBSITE RATHER THAN FULL TEXT PAGE
    for item in p_list:
        if item.text.strip().lower() == 'status information':
            num = p_list.index(item) + 1
            sponsors_list = p_list[num].text.split('\n')
            for el in sponsors_list:
                if 'Sponsors:' in el:
                    sponsors_raw = el.replace('Sponsors:', ' ').strip().replace('\r', ' ')
                    if 'Committee' in el:
                        row.committee = [{
                            'chamber': chamber_origin,
                            'committee': sponsors_raw.replace('Committee', '')
                        }]
                    else:
                        sponsors = edit_sponsors(sponsors_raw)

    if not sponsors:
        pass
    elif len(sponsors) == 1:
        principal_sponsor = sponsors[0].title()
        row.principal_sponsor = principal_sponsor
        if ' ' in principal_sponsor:
            try:
                p_sponsor_split = principal_sponsor.split()
                if p_sponsor_split[1] == 'I' or p_sponsor_split[1] == 'A' or p_sponsor_split[1] == 'E':
                    print(f'\n{p_sponsor_split}\n')
                legislators_dict = scraper_utils.search_for_legislators(name_last=p_sponsor_split[1]).to_dict('records')
                if len(legislators_dict) == 1:
                    row.principal_sponsor_id = legislators_dict[0]['goverlytics_id']
                else:
                    params = [x for x in p_sponsor_split[0].split('.') if x]
                    if len(params) == 1:
                        middle_name = False
                    else:
                        middle_name = True
                    for item in legislators_dict:
                        if middle_name:
                            if item['name_middle'][0].upper() == params[1] and item['name_first'][0].upper() == params[
                                0]:
                                if item['role'] == role:
                                    row.principal_sponsor_id = item['goverlytics_id']
            except:
                pass

        else:
            search_for = dict(name_last=principal_sponsor.title(), role=role)
            row.principal_sponsor_id = scraper_utils.get_legislator_id(**search_for)

    else:
        row.sponsors = sponsors
        sponsor_ids = []
        for sponsor in sponsors:
            if ' ' in sponsor:
                try:
                    sponsor_split = sponsor.split()
                    if sponsor_split[1] == 'I' or sponsor_split[1] == 'A' or sponsor_split[1] == 'E':
                        print(f'\n{sponsor_split}\n{sponsor}\n{url}\n')
                    legislators_dict = scraper_utils.search_for_legislators(name_last=sponsor_split[1]).to_dict(
                        'records')
                    if len(legislators_dict) == 1:
                        sponsor_ids.append(legislators_dict[0]['goverlytics_id'])
                    else:
                        params = [x for x in sponsor_split[0].split('.') if x]
                        if len(params) == 1:
                            middle_name = False
                        else:
                            middle_name = True
                        for item in legislators_dict:
                            if middle_name:
                                if item['name_middle'][0].upper() == params[1] and \
                                        item['name_first'][0].upper() == params[0]:
                                    if item['role']:
                                        sponsor_ids.append(item['goverlytics_id'])
                except:
                    pass

            else:
                search_for = dict(name_last=sponsor, role=role)
                sponsor_ids.append(scraper_utils.get_legislator_id(**search_for))
        row.sponsors_id = sponsor_ids

    row.goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'
    scraper_utils.crawl_delay(crawl_delay)
    print(f'done row for {bill_name}')
    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    start_time = time.time()
    daily_bills = get_daily_bills([senate_bills, house_bills])
    print('done daily bills')

    with Pool() as pool:
        bill_links = pool.map(get_bill_links, daily_bills)
    bill_links = list(chain(*bill_links))

    # print(len(bill_links))
    # print(f'TIME: {time.time() - start_time} SECONDS')
    #
    # data = []
    # split_bill_links = list(split(bill_links, 10))

    with Pool() as pool:
        data = pool.map(scrape, bill_links)
    # data = [scrape(x) for x in bill_links]

    # Once we collect the data, we'll write it to the database.
    scraper_utils.write_data(data)

    print('Complete!')
