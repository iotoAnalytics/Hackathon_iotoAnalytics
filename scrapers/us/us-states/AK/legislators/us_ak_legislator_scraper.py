'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import os
import sys
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

import pandas as pd
from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
import request_url
from multiprocessing import Pool
from database import Database
import re
from pprint import pprint


state_abbreviation = 'AK'
database_table_name = 'us_ak_legislators_test'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

house_url = "http://akleg.gov/house.php"
senate_url = "http://akleg.gov/senate.php"
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                        'Chrome/87.0.4280.88 Safari/537.36'}
crawl_delay = scraper_utils.get_crawl_delay(house_url)
# variables to find committee information for current/last terms; change as necessary
current_term = '32nd'
last_term = '31st'
current_year = 2021


# Get html from Alaskan legislator webpage
def get_html(url):
    url_request = request_url.UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    url_table = url_soup.find_all('ul', attrs={'class': 'people-holder'})
    return url_table[1]


# get names of legislators
def get_name(html):
    name_lst = []
    html_mod = html.find('ul', attrs={'class': 'item'}).find_all('li')
    for a in html_mod:
        name_lst.append(a.find('strong', attrs={'class': 'name'}).text)
    return name_lst


def split_names(name_lst):
    f_name = []
    l_name = []
    for name in name_lst:
        name_split = name.split()
        f_name.append(name_split[0])
        if len(name_split) == 2:
            l_name.append(name_split[1])
        if len(name_split) > 2:
            name_split.pop(0)
            temp = ''
            for item in name_split:
                temp = temp + item + ' '
            l_name.append(temp.strip())
    return [f_name, l_name]


# Helper func for getting place info
def get_info(html, i):
    a = []
    for b in html[i].find_all('dd'):
        a.append(b.text)
    return a


# grab place info
def place_info(html, lst):
    html_mod = html.find('ul', attrs={'class': 'item'}).find_all('li')
    city = []
    party = []
    district = []
    for i in range(0, len(lst)):
        temp = get_info(html_mod, i)
        city.append(temp[0])
        party.append(temp[1])
        district.append(temp[2])
    return [city, party, district]


# get emails and legislator webpage links
def get_links(html):
    links = html.find_all('a')
    email = []
    recurse = []
    for i in range(0, len(links)):
        if 'mailto:' in links[i].get('href'):
            email.append(links[i].get('href').replace('mailto:', ''))
        else:
            recurse.append(links[i].get('href'))
    return [email, recurse]


# Find phone number of legislators
def find_phone(link):
    lst = []
    url_request = request_url.UrlRequest.make_request(link, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    url_summary = url_soup.find('div', attrs={'class': 'tab-content'})

    contact_1 = url_summary.find_all('div', attrs={'class': 'bioleft'})
    contact_2 = url_summary.find_all('div', attrs={'class': 'bioright'})
    try:
        jlk = contact_1[1].text.split('\n')
        lst.append({'office': jlk[1].replace('\r', ''),
                   'phone': jlk[4].replace('Phone: ', '').strip()})
    except IndexError:
        lst.append({'office': '', 'phone': ''})
    try:
        klk = contact_2[1].text.split('\n')
        lst.append({'office': klk[1].replace('\r', ''),
                   'phone': klk[4].replace('Phone: ', '').strip()})
    except IndexError:
        lst.append({'office': '', 'phone': ''})
    return lst


# find years of service info
def find_years(link):
    lst = []
    url_request = request_url.UrlRequest.make_request(link, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_summary = url_soup.find('p')
    year_str = url_summary.text.split('\n')[4]
    x = re.findall("\d{4}-\d{4}", year_str)
    for item in x:
        if '/' in item:
            pass
        else:
            item = item.split('-')
            lst = lst + \
                [t for t in range(int(item[0]), int(item[1]))] + [int(item[1])]
    lst = [t for t in lst if t <= current_year]
    scraper_utils.crawl_delay(crawl_delay)
    return sorted(lst)


# Helper Function
def get_com_info(url):
    lst = []
    url_request = request_url.UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_summary = url_soup.find('ul', {'style': 'list-style:none'})
    if url_summary is None:
        # print('No committees found')
        pass
    else:
        for item in url_summary.find_all('li'):
            temp = item.text.split('\n')
            dict_items = [x for x in temp if x]
            com_dict = {
                'role': dict_items[0], 'committee': dict_items[1] + ' ' + dict_items[2]}
            lst.append(com_dict)
    scraper_utils.crawl_delay(crawl_delay)
    return lst


def find_com(link):
    url_request = request_url.UrlRequest.make_request(link, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    tab_html = url_soup.find(
        'div', {'class': '', 'id': 'tab7_2'}).find_all('li')
    lst = []
    split_url = link.split(re.search('\d\d', link).group())
    for item in tab_html:
        if current_term in item.text:
            com_link = split_url[0] + current_term.replace(re.search('[a-z]{2}', current_term).group(), '') + split_url[
                1]
            lst = lst + get_com_info(com_link)
        if last_term in item.text:
            com_link = split_url[0] + last_term.replace(
                re.search('[a-z]{2}', last_term).group(), '') + split_url[1]
            lst = lst + get_com_info(com_link)
    scraper_utils.crawl_delay(crawl_delay)
    return lst


# Get links from wikipedia
def get_wiki_links(data_dict_item):
    url = 'https://en.wikipedia.org/wiki/Alaska_House_of_Representatives'
    url_request = request_url.UrlRequest.make_request(url, header)
    scraper_utils.crawl_delay(crawl_delay)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_table = url_soup.find_all('span', attrs={'class': 'fn'})
    url2 = 'https://en.wikipedia.org/wiki/Alaska_Senate'
    url_request2 = request_url.UrlRequest.make_request(url2, header)
    scraper_utils.crawl_delay(crawl_delay)
    url_soup2 = BeautifulSoup(url_request2.content, 'lxml')
    url_table2 = url_soup2.find_all('span', attrs={'class': 'fn'})
    for item in url_table + url_table2:
        if item.text == data_dict_item['Full Name']:
            data_dict_item['wiki'] = 'https://en.wikipedia.com' + \
                item.find('a').get('href')


# get occupation
def get_occ(url):
    occ_lst = []
    url_request = request_url.UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_table = url_soup.find_all('tr')
    for occ in url_table:
        try:
            if occ.find('th').text == 'Occupation':
                for thing in occ.find('td').text.split(','):
                    occ_lst.append(thing.strip())
                    # print('code runs here')
        except:
            pass
    scraper_utils.crawl_delay(crawl_delay)
    return occ_lst


# get birthdays
def get_birthday(url):
    url_request = request_url.UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_table = url_soup.find_all('tr')
    bday = None
    for bd in url_table:
        try:
            if bd.find('th').text == 'Born':
                bday = re.findall('\d{4}-\d\d-\d\d', bd.find('td').text)
                if len(bday) == 0:
                    bday = None
                else:
                    bday = bday[0]
        except:
            pass
    scraper_utils.crawl_delay(crawl_delay)
    return bday


# get education
def get_education(url):
    uni = []
    deg = []
    dict_list = []
    url_request = request_url.UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_table = url_soup.find('table', attrs={'class': 'infobox vcard'})
    try:
        test_lst = url_table.find_all('tr')
        for _ in range(0, len(test_lst)):
            html_attrs = test_lst[_].find('th', attrs={'scope': 'row'})
            try:
                if html_attrs.text == 'Education' or html_attrs.text == 'Alma mater':
                    for item in test_lst[_].find('td').find_all('a'):
                        if 'University' in item.get('title') or 'College' in item.get('title'):
                            uni.append(item.get('title'))
                        else:
                            deg.append(item.text)
            except:
                pass
    except AttributeError:
        pass

    if len(uni) == len(deg) and len(uni) != 0:
        for _ in range(0, len(uni)):
            dict_list.append({"level": deg[_], "field": "", "school": uni[_]})
    elif len(deg) == 0 and len(uni) != 0:
        for _ in range(0, len(uni)):
            dict_list.append({"level": "", "field": "", "school": uni[_]})
    else:
        dict_list.append({"level": "", "field": "", "school": ""})
    scraper_utils.crawl_delay(crawl_delay)
    return dict_list


def get_gov_dicts():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    house_html = get_html(house_url)
    senate_html = get_html(senate_url)
    h_name = get_name(house_html)
    s_name = get_name(senate_html)
    h_fname = split_names(h_name)[0]
    h_lname = split_names(h_name)[1]
    s_fname = split_names(s_name)[0]
    s_lname = split_names(s_name)[1]
    h_city = place_info(house_html, h_name)[0]
    h_party = place_info(house_html, h_name)[1]
    h_district = place_info(house_html, h_name)[2]
    s_city = place_info(senate_html, s_name)[0]
    s_party = place_info(senate_html, s_name)[1]
    s_district = place_info(senate_html, s_name)[2]
    h_email = get_links(house_html)[0]
    h_urls = get_links(house_html)[1]
    s_email = get_links(senate_html)[0]
    s_urls = get_links(senate_html)[1]
    h_reps = ['Representative' for x in h_name]
    s_reps = ['Senator' for x in s_name]

    full_dict = {'Full Name': h_name + s_name, 'First Name': h_fname + s_fname, 'Last Name': h_lname + s_lname,
                 'City': h_city + s_city, 'Party': h_party + s_party, 'District': h_district + s_district,
                 'Role': h_reps + s_reps, 'Email': h_email + s_email, 'URL': h_urls + s_urls,
                 'wiki': [None for x in h_urls + s_urls]}
    d = pd.DataFrame(full_dict)
    print('Made the dictionaries!')
    return d.to_dict('records')


def scrape_gov(data_dict):
    '''
    Insert logic here to scrape all URLs acquired in the get_urls() function.

    Do not worry about collecting the goverlytics_id, date_collected, country, country_id,
    state, and state_id values, as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.
    '''
    url = data_dict['URL']

    row = scraper_utils.initialize_row()

    row.name_full = data_dict['Full Name']
    row.name_last = data_dict['Last Name'].title()
    row.name_first = data_dict['First Name']
    row.district = data_dict['District']
    row.email = data_dict['Email']
    row.source_url = data_dict['URL']
    row.role = data_dict['Role']

    row.phone_numbers = find_phone(url)
    row.committees = find_com(url)
    row.years_active = find_years(url)

    if data_dict['Party'] == 'Democrat' or data_dict['Party'] == 'Republican' or data_dict['Party'] == 'Independent':
        party = data_dict['Party']
    else:
        party = 'No Affiliation'

    row.party = party
    row.party_id = scraper_utils.get_party_id(party)

    # wiki info
    get_wiki_links(data_dict)
    if data_dict['wiki'] is not None:
        row.birthday = get_birthday(data_dict['wiki'])
        row.occupation = get_occ(data_dict['wiki'])
        row.education = get_education(data_dict['wiki'])

    print('hit')
    return row


if __name__ == '__main__':
    dict_lst = get_gov_dicts()[0:5]

    with Pool() as pool:
        data = pool.map(scrape_gov, dict_lst)
        print('done scraping!')

    scraper_utils.write_data(data)

    print('Complete!')
