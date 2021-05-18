import sys
import os
from pathlib import Path
from nameparser import HumanName
from bs4 import BeautifulSoup
from scraper_utils import USStateLegislatorScraperUtils
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

base_url = 'https://www.nmlegis.gov'
wiki_url = 'https://en.wikipedia.org/'

scraper_utils = USStateLegislatorScraperUtils('NM', 'nm_sc_legislators')
crawl_delay = scraper_utils.get_crawl_delay(base_url)

senators_and_reps = ['/Members/Legislator_List?T=R', '/Members/Legislator_List?T=S']
senators_and_reps_wiki = ['/wiki/New_Mexico_House_of_Representatives', '/wiki/New_Mexico_Senate']


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
    Takes base URL of gov site, combine with senate OR house paths to get individual representative page URLS.

    :return: a list of representative source URLS
    """

    urls = []
    scrape_url = base_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'lxml')
    content_table = soup.find('div', {'class': 'panel-body'})
    links = content_table.find_all('a', {'class': 'thumbnail text-center'})

    pbar = tqdm(links)
    for link in pbar:
        urls.append(base_url + '/Members/' + link.get('href'))
        scraper_utils.crawl_delay(crawl_delay)

    return urls


def join_senators_and_reps():
    """
    Joins the gov urls scraped from senator and representative paths (global value).

    :return: Joined list of all legislators gov urls.
    """

    all_urls = []
    for path in senators_and_reps:
        all_urls += get_urls(path)
    return all_urls


def get_wiki_urls(path):
    """
    Takes base URL of wiki site, combine with senate OR house paths to get individual representative page URLS.

    :return: a list of representative source URLS
    """

    urls = []
    scrape_url = wiki_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'lxml')
    content_table = soup.find('table', {'class': 'wikitable sortable'})
    rows = content_table.find('tbody').find_all('tr')

    pbar = tqdm(range(1, len(rows)))
    for row in pbar:
        href = rows[row].find_all('td')[1].find('a').get('href')
        link = wiki_url + href
        urls.append(link)
        scraper_utils.crawl_delay(crawl_delay)

    return urls


def join_senators_and_reps_wiki():
    """
    Joins the wiki urls scraped from senator and representative wiki paths (global value).

    :return: Joined list of all legislators wiki urls.
    """

    all_urls = []
    for path in senators_and_reps_wiki:
        all_urls += get_wiki_urls(path)
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
    occupations = []
    if content.text != '':
        occupations.append(content.text)

    row.occupation = occupations


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


def set_phone_numbers(row, soup):
    """
    Mutate legislator row and sets phone number values.

    :param row: row of legislator
    :param soup: soup object using respective legislator url
    """

    numbers_lst = []

    capitol_num = soup.find('span', {'id': 'MainContent_formViewLegislator_lblCapitolPhone'}).text
    office_num = soup.find('span', {'id': 'MainContent_formViewLegislator_lblOfficePhone'}).text
    home_num = soup.find('span', {'id': 'MainContent_formViewLegislator_lblHomePhone'}).text

    capitol_dict = {'number': capitol_num, 'office': 'capitol office'}
    office_dict = {'number': office_num, 'office': 'personal office'}
    home_dict = {'number': home_num, 'office': 'home'}

    temp = [capitol_dict, office_dict, home_dict]

    for item in temp:
        if item['number']:
            numbers_lst.append(item)

    row.phone_numbers = numbers_lst


def set_addresses(row, soup):
    """
    Mutate legislator row and sets phone number values.

    :param row: row of legislator
    :param soup: soup object using respective legislator url
    """

    content = soup.find('span', {'id': 'MainContent_formViewLegislator_lblAddress'}).text
    address = {'address': content, 'location': ''}

    if address['address'].strip() != ',':
        addresses = [address]
        row.addresses = addresses


def set_email(row, soup):
    """
    Mutate legislator row and sets email values.

    :param row: row of legislator
    :param soup: soup object using respective legislator url
    """

    content = soup.find('a', {'id': 'MainContent_formViewLegislator_linkEmail'}).text

    row.email = content


def set_committees(row, soup):
    """
    Mutate legislator row and sets committee values.

    :param row: row of legislator
    :param soup: soup object using respective legislator url
    """

    all_committees = []

    standing_committee = soup.find('table', {'id': 'MainContent_formViewLegislator_gridViewStandingCommittees'})
    interim_committee = soup.find('table', {'id': 'MainContent_formViewLegislator_gridViewInterimCommittees'})

    standing_rows = standing_committee.find_all('tr')
    for r in standing_rows:
        committees = [word for word in r.text.split('\n') if word]
        role_and_com = {'role': committees[0], 'committee': committees[1].lower().title()}
        all_committees.append(role_and_com)

    interim_rows = interim_committee.find_all('tr')
    for r in interim_rows:
        committees = [word for word in r.text.split('\n') if word]
        role_and_com = {'role': committees[0], 'committee': committees[1].lower().title()}
        all_committees.append(role_and_com)

    row.committees = all_committees


def create_rows(length):
    """
    Create rows for each legislator.

    :param length: length of all legislators (senators and reps)
    :return: list of rows
    """

    return [scraper_utils.initialize_row() for _ in range(length)]


def scrape_gov_site(all_gov_urls, rows):
    """
    Fill in rows from gov site.

    :param all_gov_urls: all URLS from gov site
    :param rows: list of unfilled rows
    :return: list of rows filled with all data scraped from gov site sorted by legislator role, then district number.
    """

    pbar = tqdm(range(len(rows)))
    pbar_test = tqdm(range(15))

    for item in pbar:
        soup = make_soup(all_gov_urls[item])
        set_source_id(all_gov_urls[item], rows[item])
        set_source_url(all_gov_urls[item], rows[item])
        set_name_info(rows[item], soup)
        set_party_info(rows[item], soup)
        set_occupation(rows[item], soup)
        set_role(rows[item], soup)
        set_district(rows[item], soup)
        set_phone_numbers(rows[item], soup)
        set_email(rows[item], soup)
        set_addresses(rows[item], soup)
        set_committees(rows[item], soup)

    return sorted(rows, key=lambda row: (row.role, int(row.district)))


def scrape_wiki_site(all_wiki_urls, sorted_rows):
    """
    Take rows that are partially filled and grabs any other missing information from wikipedia (birthday, years served,
    etc).

    :param all_wiki_urls: all URLS from wiki site
    :param sorted_rows: list of partially filled rows
    :return:
    """

    pbar = tqdm(range(len(sorted_rows)))
    for item in pbar:
        wiki_info = scraper_utils.scrape_wiki_bio(all_wiki_urls[item])
        sorted_rows[item].education = wiki_info['education']

        if wiki_info['birthday'] is not None:
            sorted_rows[item].birthday = str(wiki_info['birthday'])
        sorted_rows[item].years_active = wiki_info['years_active']
        sorted_rows[item].most_recent_term_id = wiki_info['most_recent_term_id']


def organize_data():
    """
    Organize flow of helper functions and row setting.
    1. Get all gov and wiki urls
    2. Create all rows
    3. Scrape gov urls and fill rows
    4. Scrape wiki urls and fill rows
    5. Write to data
    """

    all_gov_urls = join_senators_and_reps()
    all_wiki_urls = join_senators_and_reps_wiki()
    rows = create_rows(len(all_gov_urls))

    unfinished_rows = scrape_gov_site(all_gov_urls, rows)
    scrape_wiki_site(all_wiki_urls, unfinished_rows)
    scraper_utils.write_data(rows, 'us_nm_legislators')


def main():
    """
    Driver
    """
    organize_data()


if __name__ == '__main__':
    main()
