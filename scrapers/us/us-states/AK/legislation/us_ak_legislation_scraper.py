'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import USStateLegislationScraperUtils
from database import Database
import configparser
import re
import requests
from bs4 import BeautifulSoup
import request_url
import pandas as pd
from multiprocessing import Pool
import time


header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}

# # Initialize config parser and get variables from config file
# configParser = configparser.RawConfigParser()
# configParser.read('config.cfg')

state_abbreviation = 'AK'
# database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
# legislator_table_name = str(configParser.get('scraperConfig', 'legislator_table_name'))

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, 'us_ak_legislation', 'us_ak_legislators')

base_url = 'http://www.akleg.gov/basis/Home/BillsandLaws'
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def past_terms_url(url):
    url_request = request_url.UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    href = url_soup.find('div', {'id': 'fullpage'}).find_all('li')[
        1].find('a').get('href')
    scraper_utils.crawl_delay(crawl_delay)
    return 'http://www.akleg.gov' + href


def get_session(url):
    url_request = request_url.UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_text = url_soup.find('em', {'class': 'date'}).text
    session = re.findall("\d{4}-\d{4}", url_text)[0]
    scraper_utils.crawl_delay(crawl_delay)
    return session


def get_html(url):
    url_request = request_url.UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_table = url_soup.find('table')
    scraper_utils.crawl_delay(crawl_delay)
    return url_table


def fix_p_sponsors_column(k):
    a = k.to_list()
    i = 0
    while i < len(a):
        try:
            if 'REPRESENTATIVE ' in a[i]:
                a[i] = a[i].replace('REPRESENTATIVE ', '')
            elif 'SENATOR ' in a[i]:
                a[i] = a[i].replace('SENATOR ', '')
            i = i + 1
        except TypeError:
            a[i] = ' '
    return a


def get_links(url_table):
    links = []
    url_links = url_table.find_all('a')
    for link in url_links:
        temp = 'http://www.akleg.gov' + link.get(
            'href') + '#tab1_4'  # if can't go to link add http:// -> idk why there's a diff
        links.append(temp)
    return links


def go_into_links(link):
    idict = {"cosponsors": 'NONE', "bill summary": 'NONE'}
    url_request = request_url.UrlRequest.make_request(link, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_summary = url_soup.find_all(
        'div', attrs={'class': 'information-holder'})
    a = url_summary[1].text.split('\n')
    lst = [x for x in a if x]
    try:
        idict['bill summary'] = lst[-1].replace('"', '')
    except IndexError:
        pass
    for sum in url_summary:
        for item in sum.find_all('li'):
            if 'Sponsor(S)' in item.text:
                cosponsors = item.text.replace(
                    'Sponsor(S)', '').replace('\n', '').strip()
                idict['cosponsors'] = cosponsors
    scraper_utils.crawl_delay(crawl_delay)
    return idict


def split_url_lists(url_lst):
    lst = []
    temp = []
    i = 0
    while i < len(url_lst):
        temp.append(url_lst[i])
        i = i + 1
        if len(temp) == 10 or i == len(url_lst):
            lst.append(temp)
            temp = []
    return lst


def get_bill_text(url):
    try:
        url_request = request_url.UrlRequest.make_request(url, header)
        url_soup = BeautifulSoup(url_request.content, 'lxml')
        scraper_utils.crawl_delay(crawl_delay)
        bill_url = url_soup.find(
            'td', {'data-label': 'Version'}).find('a').get('href')
        bill_url = 'http://www.akleg.gov' + bill_url

        url_request = request_url.UrlRequest.make_request(bill_url, header)
        scraper_utils.crawl_delay(crawl_delay)
        url_soup = BeautifulSoup(url_request.content, 'lxml')
        url_sum = url_soup.find('div', {'id': 'draftOverlay'}).text
        text = url_sum.replace('\n', '').split('\r')
        text = [re.sub('\d{2}', '', x).strip() for x in text if x]
        return ' '.join(text)
    except AttributeError:
        return ''


def split_cosponsors(page_info):
    r = 'REPRESENTATIVE'
    s = 'SENATOR'
    reps = []
    sens = []
    temp = page_info.split(s)
    if len(temp) == 2:
        if '' == temp[0]:
            sens = temp[1].strip().split(',')[1:]
            sens = [x.strip() for x in sens if x]
        else:
            reps = temp[0].strip().split(',')[1:]
            reps = [x.strip() for x in reps if x]
            sens = temp[1].strip().split(',')
            if 'S ' in sens[0]:
                sens[0] = sens[0].replace('S ', '')
            sens = [x.strip() for x in sens if x]
    if len(temp) == 1:
        if r in temp[0]:
            reps = temp[0].strip().split(',')[1:]
            reps = [x.strip() for x in reps if x]
    return {'Representatives': reps, 'Senators': sens, 'total': reps + sens}


def get_dictionaries():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''

    gov_url = past_terms_url(base_url)
    url_table = get_html(gov_url)
    df = pd.read_html(str(url_table))[0]
    del df['Unnamed: 3']
    del df['Current Status']
    del df['Status Date']
    df = df.rename(columns={'Short Title': 'Site Topic',
                   'Prime Sponsor(s)': 'Principal Sponsors'})
    # df['Principal Sponsors'] = fix_p_sponsors_column(df['Principal Sponsors'])
    links = get_links(url_table)
    df['urls'] = links
    data_dict = df.to_dict('records')
    return data_dict


def scrape(data_dict):

    row = scraper_utils.initialize_row()
    url = data_dict['urls']
    #print('doing url: ' + url)
    temp_dict = go_into_links(url)
    cosponsors = split_cosponsors(temp_dict['cosponsors'])
    session = get_session(url)

    bill_name = data_dict['Bill']
    # p_sponsor = data_dict['Principal Sponsors'].replace('REPRESENTATIVE', '').replace('SENATOR', '').title().strip()
    goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'

    row.goverlytics_id = goverlytics_id
    # row.url = f'/us/{state_abbreviation}/legislation/{goverlytics_id}'
    row.bill_name = bill_name
    row.source_topic = data_dict['Site Topic']
    # row.principal_sponsor = p_sponsor
    row.source_url = url
    row.cosponsors = cosponsors['total']
    row.bill_summary = temp_dict['bill summary']
    row.bill_text = get_bill_text(url)

    # find sponsor ID:
    if data_dict['Site Topic'] != 'NOT INTRODUCED':
        p_sponsor = data_dict['Principal Sponsors'].replace(
            'REPRESENTATIVE', '').replace('SENATOR', '').title().strip()
        row.principal_sponsor = p_sponsor
        if 'REPRESENTATIVE' in data_dict['Principal Sponsors']:
            row.principal_sponsor_id = scraper_utils.get_legislator_id(
                role='Representative', name_last=p_sponsor)
        elif 'SENATOR' in data_dict['Principal Sponsors']:
            row.principal_sponsor_id = scraper_utils.get_legislator_id(
                role='Senator', name_last=p_sponsor)

    # find cosponsor ID:
    c_id = []
    for item in cosponsors['Representatives']:
        c_id.append(scraper_utils.get_legislator_id(
            role='Representative', name_last=item.title()))
    for item in cosponsors['Senators']:
        c_id.append(scraper_utils.get_legislator_id(
            role='Senator', name_last=item.title()))
    row.cosponsors_id = c_id

    print("done row for: " + row.bill_name)
    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    dictionaries = get_dictionaries()
    print('grabbed dictionaries')

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, dictionaries)
    print('done scrapping')

    # Once we collect the data, we'll write it to the database.
    scraper_utils.write_data(data)

    print('Complete!')
