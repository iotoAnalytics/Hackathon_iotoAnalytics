import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))
from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from datetime import *
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3
import urllib3

# Other import statements
urllib3.disable_warnings()

state_abbreviation = 'OH'
database_table_name = 'us_oh_legislation'
legislator_table_name = 'us_oh_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

legislation_links = 'https://ohiohouse.gov/legislation/search?legislationTypes=Bill,Resolution,Joint%20Resolution,' \
                    'Concurrent%20Resolution&start=1&pageSize=100&sort=Number '
legislation_base = 'https://ohiohouse.gov'
bill_base = 'https://ohiohouse.gov/legislation/'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(legislation_base)


# Recursively collects all bill links from site
def get_bill_links(url):
    bill_links = []
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    info_box = soup.find('div', {'class': 'search-results'})
    bill_rows = info_box.find('table', {'class': 'data-grid legislation-table'}).find('tbody').find_all('tr')
    for tr in bill_rows:
        title = tr.find('td', {'class': 'title-cell'}).text
        bill_links.append({
            'link': bill_base + tr.find('a').get('href'),
            'title': title
        })
    button_text = info_box.find('div', {'class': 'page-selector'}).find('button').text
    button_text = [int(x) for x in re.findall('[0-9]+', button_text)]
    if button_text[0] == button_text[1]:
        return bill_links
    else:
        next_url = info_box.find('div', {'class': 'compact-pager'}).find('a', {'class': 'next'}).get('href')
        return bill_links + get_bill_links(legislation_base + next_url)


def filter_name(name):
    return name.replace('\n', '').replace('\xa0', '').split('District')[0]


def fix_sponsor_link(url):
    if 'http' not in url and 'members' in url:
        members_link = legislation_base.replace('https', 'http') + url
        return members_link.replace('members/', '').replace('www.', '')
    return url.replace('https', 'http').replace('www.', '')


def edit_date(date):
    date_split = date.split('/')
    return f'{date_split[2]}-{date_split[0]}-{date_split[1]}'


def compare_dates(action_one, action_two):
    y1, m1, d1 = [int(x) for x in action_one['date'].split('-')]
    y2, m2, d2 = [int(x) for x in action_two['date'].split('-')]
    if datetime(y1, m1, d1) > datetime(y2, m2, d2):
        return action_one
    return action_two


# Make sure to pass in url that the fix_sponsor_link function has edited
def get_id(fixed_link):
    goverlytics_id = scraper_utils.get_legislator_id(source_url=fixed_link)
    if not goverlytics_id:
        name = ' '.join([x.title() for x in fixed_link.split('/')[-1].split('-')])
        hn = HumanName(name)
        ln = "O'Brien" if hn.last == 'Obrien' else hn.last
        goverlytics_id = scraper_utils.get_legislator_id(name_first=hn.first, name_last=ln, role='Senator')
        if goverlytics_id:
            print(f'Found ID for {name}')
    return goverlytics_id


def get_votes(votes):
    vote_soup = BeautifulSoup(requests.get(votes).content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    try:
        vote_table = vote_soup.find('table', {'class': 'data-grid legislation-votes-table unhighlighted-table'}).find(
            'tbody').find_all('tr', recursive=False)
    except AttributeError:
        return []
    vote_info_lst = []

    for table_row in vote_table:
        vote_date = edit_date(table_row.find('th', {'class': 'date-cell'}).text)
        vote_chamber = table_row.find('td', {'class': 'chamber-cell'}).text
        vote_result = table_row.find('td', {'class': 'result-cell'}).text
        vote_html = table_row.find('td', {'class': 'vote-cell'})
        tables = vote_html.find('div', {'class': 'vote-breakdown'}).find_all('table')
        yeas, nays = 0, 0
        vote_lst = []
        for item in vote_html.find_all('span')[:-1]:
            if 'Yeas' in item.text:
                yeas = int(re.search('[0-9]+', item.text).group())
            elif 'Nays' in item.text:
                nays = int(re.search('[0-9]+', item.text).group())
        if len(tables) == 1:
            if yeas == 0:
                votetext = 'nay'
            elif nays == 0:
                votetext = 'yea'
            for legislator in tables[0].find_all('a'):
                goverlytics_id = get_id(fix_sponsor_link(legislator.get('href')))
                vote_lst.append({
                    'goverlytics_id': goverlytics_id,
                    'legislator': legislator.text,
                    'votetext': votetext
                })
        elif len(tables) == 2:
            for table in tables:
                for legislator in table.find_all('a'):
                    goverlytics_id = get_id(fix_sponsor_link(legislator.get('href')))
                    vote_lst.append({
                        'goverlytics_id': goverlytics_id,
                        'legislator': legislator.text,
                        'votetext': 'yea' if tables.index(table) == 0 else 'nay'
                    })

        vote_info = {
            'date': vote_date,
            'description': vote_result,
            'yea': yeas,
            'nay': nays,
            'nv': 0,
            'absent': 0,
            'total': yeas + nays,
            'passed': 1 if yeas >= nays else 0,
            'chamber': vote_chamber,
            'votes': vote_lst
        }
        vote_info_lst.append(vote_info)

    return vote_info_lst


def scrape(info_dict):
    row = scraper_utils.initialize_row()

    url = info_dict['link']
    print(f'\nscraping {url}...\n')
    row.source_url = url
    row.bill_summary = info_dict['title']
    temp = url.replace(bill_base, '').split('/')
    session = temp[0]
    bill_name = temp[1].upper()
    row.session = session
    row.bill_name = bill_name
    row.goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'
    # site only has bills or resolutions, but might need to change this part in the future
    row.bill_type = 'Bill' if re.search('[A-Z]+', bill_name).group()[-1] == 'B' else 'Resolution'

    bill_soup = BeautifulSoup(requests.get(url).content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)

    try:
        a_tags = bill_soup.find('section', {'class': 'legislation-general-info legislation-info-module'}).find_all('a', {
            'class': 'tag-link'})
        subjects = []
        for tag in a_tags:
            if 'subjects' in tag.get('href'):
                subjects.append(tag.text)
        row.source_topic = ', '.join(subjects)
    except AttributeError:
        pass

    try:
        text_link = bill_soup.find('a', {'class': 'tag-link', 'target': '_blank'}).get('href')
        text_link = text_link.replace('format=pdf', 'format=html') if text_link else ''
        if text_link:
            bill_text = BeautifulSoup(requests.get(text_link, verify=False).content, 'lxml').text \
                .replace('\n', ' ').replace('\t', ' ').strip()
            scraper_utils.crawl_delay(crawl_delay)
            row.bill_text = bill_text
    except AttributeError:
        pass

    status = url + '/status'
    votes = url + '/votes'

    # scrape the status portion of website
    status_soup = BeautifulSoup(requests.get(status).content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    try:
        primary_sponsor_section = status_soup.find('section', {'class': 'legislation-primary-sponsors'})
        sponsor_lst = primary_sponsor_section.find_all('a')
        if len(sponsor_lst) > 1:
            sponsor_name_lst, sponsor_id_lst = [], []
            for sponsor in sponsor_lst:
                sponsor_name_lst.append(filter_name(sponsor.text))
                sponsor_id_lst.append(get_id(fix_sponsor_link(sponsor.get('href'))))
            row.sponsors = sponsor_name_lst
            row.sponsors_id = sponsor_id_lst
        else:
            row.principal_sponsor = filter_name(sponsor_lst[0].text)
            row.principal_sponsor_id = get_id(fix_sponsor_link(sponsor_lst[0].get('href')))
    except AttributeError:
        pass

    try:
        cosponsor_section = status_soup.find('section', {'class': 'legislation-cosponsors'})
        if cosponsor_section:
            cosponsor_names_lst, cosponsor_ids_lst = [], []
            for cosponsor in cosponsor_section.find_all('a'):
                cosponsor_id = get_id(fix_sponsor_link(cosponsor.get('href')))
                cosponsor_ids_lst.append(cosponsor_id)
                cosponsor_name = cosponsor.text
                cosponsor_names_lst.append(cosponsor_name)
            row.cosponsors = cosponsor_names_lst
            row.cosponsors_id = cosponsor_ids_lst
    except AttributeError:
        pass

    try:
        status_html = status_soup.find('section', {'class': 'legislation-info-module'}).find('tbody')
        if status_html:
            status_lst = status_html.find_all('tr')
            actions, committees = [], []
            current_action, date_introduced = '', None
            for status_el in status_lst:
                date = edit_date(status_el.find('th', {'class': 'date-cell'}).text)
                action_by = status_el.find('td', {'class': 'chamber-cell'}).text
                description = status_el.find('td', {'class': 'action-cell'}).text
                date_introduced = date if description == 'Introduced' else None
                committee = status_el.find('td', {'class': 'committee-cell'}).text
                action = {
                    'date': date,
                    'action_by': action_by,
                    'description': description
                }
                actions.append(action)
                current_action = action if current_action == '' else compare_dates(action, current_action)
                if committee:
                    committees.append({
                        'chamber': action_by,
                        'committee': committee
                    })
            current_status = current_action['description'] if current_action != '' else ''
            row.actions = actions
            row.date_introduced = date_introduced
            row.committees = [dict(t) for t in {tuple(d.items()) for d in committees}]
            row.current_status = current_status
    except AttributeError:
        pass

    # NoneType AttributeError handled by get_votes function
    row.votes = get_votes(votes)

    scraper_utils.crawl_delay(crawl_delay)

    # print(row)
    return row


if __name__ == '__main__':
    bill_links = get_bill_links(legislation_links)
    print('Done getting bill links')

    with Pool() as pool:
        data = pool.map(scrape, bill_links)

    scraper_utils.write_data(data)

    print('Complete!')

