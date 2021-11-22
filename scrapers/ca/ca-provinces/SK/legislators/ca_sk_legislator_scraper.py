import sys
import os
from pathlib import Path

p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from bs4 import BeautifulSoup
from nameparser import HumanName
from request_url import UrlRequest

from multiprocessing import Pool
from scraper_utils import CAProvTerrLegislatorScraperUtils

header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
url = 'https://www.legassembly.sk.ca/mlas/'
base_url = 'https://www.legassembly.sk.ca'
wiki_url = 'https://en.wikipedia.org/wiki/Legislative_Assembly_of_Saskatchewan'
wiki_base = 'https://en.wikipedia.org'

scraper_utils = CAProvTerrLegislatorScraperUtils('SK', 'ca_sk_legislators')
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_legislator_links(url):
    links = []
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    table = url_soup.find('table', {'id': 'MLAs'})
    for item in table.find_all('tr'):
        if item.find('td', {'class': 'mla-name'}) and "Vacant" not in item.text:
            links.append(base_url + item.find('a').get('href'))
    return links


def get_wiki_links(url):
    links = []
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    table = url_soup.find('table', {'class': 'wikitable sortable'})
    for item in table.find_all('tr'):
        if len(item.find_all('a')) > 0:
            links.append(wiki_base + item.find_all('a')[0].get('href'))
    return links


def make_diction(legis_lst, wiki_lst):
    return_lst = []
    for item in legis_lst:
        return_lst.append({'url': item, 'wiki_list': wiki_lst})
    return return_lst


def scrape(info_dict):
    url = info_dict['url']
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_header = url_soup.find('div', {
                            'class': 'mla-header'}).text.replace('\xa0', ' ').replace('\n', ' ').split(' - ')

    row = scraper_utils.initialize_row()

    name = HumanName(url_header[0])
    name_full = url_header[0].replace(name.title, '').strip()
    name_first = name.first
    name_last = name.last
    party = url_header[1].strip()
    url_riding = url_soup.find(
        'div', {'class': 'mla-constituency-cell'}).text.replace('\n', '')
    div_lst = url_soup.find('div', {'class': 'row'}).find_all(
        'div', {'class': 'col-md-4'})
    addresses = []
    email = ''
    phone = []
    for item in div_lst:
        if 'Address' in item.text:
            lst = item.find_all('div')
            address = {'location': lst[0].text, 'address': lst[1].text}
            addresses.append(address)
            for el in lst:
                if 'Phone' in el.text:
                    if el.find('span').text != '':
                        phone_dict = {
                            'office': lst[0].text, 'number': el.find('span').text}
                        phone.append(phone_dict)
        elif 'Online' in item.text:
            lst = item.find_all('div')
            for el in lst: 
                if 'E-mail' in el.text:
                    email = el.find('a').get('href')
    
    email = email.replace('mailto:', '')

    row.name_full = name_full
    row.name_first = name_first
    row.name_last = name_last
    row.name_middle = name.middle
    row.party = party
    if row.party == "New Democratic Party":
        row.party = "New Democratic"
    try:
        row.party_id = scraper_utils.get_party_id(row.party)
    except:
        row.party_id = 0
    row.riding = url_riding
    row.addresses = addresses
    row.email = email
    row.phone_numbers = phone
    row.source_url = url
    for item in info_dict['wiki_list']:
        if (name_first in item and name_last in item) or ("Dave" in item and name_last in item):
            wiki_info = scraper_utils.scrape_wiki_bio(item)
            row.education = wiki_info['education']
            row.birthday = wiki_info['birthday']
            row.occupation = wiki_info['occupation']
            row.years_active = wiki_info['years_active']
            row.most_recent_term_id = wiki_info['most_recent_term_id']
            row.wiki_url = wiki_info['wiki_url']

    bio = url_soup.find('div', {'class' : 'biography-cell'}).text
    row.gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last, bio)

    print('Done row for: '+name_full)
    scraper_utils.crawl_delay(crawl_delay)
    return row

try:
    if __name__ == '__main__':
        links = get_legislator_links(url)
        wiki_links = get_wiki_links(wiki_url)
        info_dict = make_diction(links, wiki_links)
        print('Made dictionaries')
        with Pool() as pool:
            data = pool.map(scrape, info_dict)
        print('Done Scraping!')
        scraper_utils.write_data(data)
        print('Complete!')
except Exception as e:
    print(e)
    sys.exit(1)