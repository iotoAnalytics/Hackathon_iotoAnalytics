import boto3
import re
from nameparser import HumanName
from pprint import pprint
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
import time
from scraper_utils import USStateLegislatorScraperUtils
import sys
import os
from pathlib import Path
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

scraper_utils = USStateLegislatorScraperUtils('NM', 'nm_sc_legislators')

base_url = 'https://www.nmlegis.gov/'

# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)

senators_and_reps = ['/Members/Legislator_List?T=S', 'Members/Legislator_List?T=R']


def make_soup(url):
    """
    Takes senator and representative paths and returns soup object.

    :param url: string representing url paths
    :return: soup object
    """

    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'lxml')
    return soup


def get_urls(path):
    """
    Takes base URL of site, combine with senate OR house paths to get individual representative page URLS.

    :return: a list of representative source URLS
    """

    urls = []
    scrape_url = base_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'lxml')
    content_table = soup.find('div', {'class': 'panel-body'})
    links = content_table.find_all('a', {'class': 'thumbnail text-center'})
    links = tqdm(links)

    for link in links:
        urls.append(base_url + '/Members/' + link.get('href'))

    scraper_utils.crawl_delay(crawl_delay)
    return urls


def join_senators_and_reps():
    """
    Joins the urls scraped from senator and representative paths (global value).

    :return: Joined list of all legislators.
    """

    all_urls = []
    for path in senators_and_reps:
        all_urls += get_urls(path)
    return all_urls


def set_source_id(url, row):
    """
    Mutate legislator row and sets source url value.

    :param url: url of legislator
    :param row: row of legislator
    """

    source_id = url.split('=')[1]
    row.source_id = source_id


def set_source_url(url, row):
    """
    Mutate legislator row and sets source url value.

    :param url: url of legislator
    :param row: row of legislator
    """

    row.source_url = url


def set_name_info(row, soup):
    """
    Mutate legislator row and sets name value.

    :param row: row of legislator
    :param soup: soup object using respective legislator url
    """

    content = soup.find('span', {'id': 'MainContent_formViewLegislatorName_lblLegislatorName'})
    name = " ".join(content.text.split('-')[0].split()[1:])  # get only the name
    human_name = HumanName(name)

    row.name_first = human_name.first
    row.name_middle = human_name.middle
    row.name_last = human_name.last
    row.name_suffix = human_name.suffix
    row.name_full = human_name.full_name


def set_party_info(row, soup):
    """
    Mutate legislator row and sets party ID value and party name at same time.

    :param row: row of legislator
    :param soup: soup object using respective legislator url
    """

    content = soup.find('span', {'id': 'MainContent_formViewLegislatorName_lblLegislatorName'})
    party = " ".join(content.text.split('-')[1].strip().strip("()"))
    party_id = scraper_utils.get_party_id('Republican' if party == 'R' else 'Democrat')
    party_name = 'Republican' if party == 'R' else 'Democrat'

    row.party = party_name
    row.party_id = party_id


def set_occupation(row, soup):
    """
    Mutate legislator row and sets occupation value.

    :param row: row of legislator
    :param soup: soup object using respective legislator url
    """

    content = soup.find('span', {'id': 'MainContent_formViewLegislator_lblOccupation'})
    occupation = content.text

    row.occupation = occupation


def set_role(row, soup):
    """
    Mutate legislator row and sets role value.

    :param row: row of legislator
    :param soup: soup object using respective legislator url
    """

    content = soup.find('span', {'id': 'MainContent_formViewLegislatorName_lblLegislatorName'})
    role = content.text.split().pop(0)
    row.role = role


def set_district(row, soup):
    """
    Mutate legislator row and sets district value.

    :param row: row of legislator
    :param soup: soup object using respective legislator url
    """

    content = soup.find('a', {'id': 'MainContent_formViewLegislator_linkDistrict'})
    district = content.text
    row.district = district


def create_rows(length):
    """
    Create rows for each legislator.

    :param length: length of all legislators (senators and reps)
    :return: list of rows
    """

    return [scraper_utils.initialize_row() for _ in range(length)]


def organize_data():
    """
    Organizes the flow of helper functions.
    """

    all_urls = join_senators_and_reps()
    rows = create_rows(len(all_urls))
    pbar = tqdm(range(len(all_urls)))
    pbar_test = tqdm(range(20))

    for item in pbar_test:
        pbar.set_description(f'Setting info for URL:{all_urls[item]}')
        soup = make_soup(all_urls[item])

        set_source_id(all_urls[item], rows[item])
        set_source_url(all_urls[item], rows[item])
        set_name_info(rows[item], soup)
        set_role(rows[item], soup)
        set_district(rows[item], soup)
        set_party_info(rows[item], soup)
        set_occupation(rows[item], soup)

    for r in rows:
        print(r)


def main():
    """
    Driver
    """

    organize_data()


if __name__ == '__main__':
    main()
