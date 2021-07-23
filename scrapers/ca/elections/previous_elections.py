import sys
import os
from pathlib import Path
import re
import datetime

NODES_TO_ROOT = 3
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from bs4 import BeautifulSoup as soup
from scraper_utils import ElectionScraperUtils
from urllib.request import urlopen

COUNTRY = 'ca'
TABLE = 'ca_previous_elections'
MAIN_URL = 'https://www.elections.ca/'
PAST_ELECTIONS_URL = MAIN_URL + 'content.aspx?section=ele&dir=pas&document=index&lang=e'

scraper_utils = ElectionScraperUtils(COUNTRY, TABLE)
crawl_delay = scraper_utils.get_crawl_delay(MAIN_URL)

def program_driver():
    general_election_data = GeneralElection().get_election_data()
    by_election_data = ByElection().get_election_data()
    data = general_election_data + by_election_data
    print("writing data")
    scraper_utils.write_data(data)
    print("complete")

class Election:
    def get_page_as_soup(self, url):
        page_html = self.__get_site_as_html(url)
        return soup(page_html, 'html.parser')

    def __get_site_as_html(self, url):
        uClient = urlopen(url)
        page_html = uClient.read()
        uClient.close()
        scraper_utils.crawl_delay(crawl_delay)
        return page_html

    def _has_official_voting_result(self, tag: soup):
        text = ' '.join(tag.text.split())
        return "Official Voting Results" in text and tag.name == 'a'

    def _get_ovr_url(self, url: str) -> str:
        link = MAIN_URL + url
        page_soup = self.get_page_as_soup(link)
        main_content = page_soup.find('div', {'id':'content-main'})
        try:
            link = main_content.find_all(self._has_official_voting_result)[0]['href']
        except Exception as e:
            return ''
        return MAIN_URL + link[1:]

class GeneralElection(Election):
    def get_election_data(self) -> list:
        page_soup = self.get_page_as_soup(PAST_ELECTIONS_URL)
        elections = self._get_elections_from_soup(page_soup)
        return self._extract_election_info(elections)

    def _get_elections_from_soup(self, page_soup: soup) -> list:
        main_content = page_soup.find('div', {'id': 'content-main'})
        return main_content.find('ul').find_all('li')

    def _extract_election_info(self, elections: list[soup]) -> list:
        data = []
        for election in elections:
            data.append(self._get_row_data(election))
        return data

    def _get_row_data(self, election: soup):
        row = scraper_utils.initialize_row()
        text = election.text
        row.election_name = self._get_election_name(text)
        row.election_date = self._get_election_date(text)
        row.official_votes_record_url = self._get_ovr_url(election.a['href'])
        row.description = f"The {row.election_name} which was held on {row.election_date}."
        row.is_by_election = False
        return row
    
    def _get_election_name(self, text: str) -> str:
        name = text.split(', ')[0]
        name = name.lower()
        name = name.replace(' ', '_')
        return name.strip()

    def _get_election_date(self, text: str) -> str:
        text = text.split('Election, ')[1]
        date = datetime.datetime.strptime(text, '%B %d, %Y')
        date = date.strftime('%Y-%m-%d')
        return date

class ByElection(Election):
    def get_election_data(self) -> list:
        page_soup = self.get_page_as_soup(PAST_ELECTIONS_URL)
        by_elections_links = self._get_by_elections_links_from_soup(page_soup)
        return self._extract_by_election_info(by_elections_links)

    def _get_by_elections_links_from_soup(self, page_soup: soup):
        main_content = page_soup.find('div', {'id': 'content-main'})
        relelvant_ul = main_content.find_all('ul')[1:]
        all_a = []
        for ul in relelvant_ul:
            all_a.extend(ul.find_all('a'))
        return all_a

    def _extract_by_election_info(self, elections: list[soup]) -> list:
        data = []
        for election in elections:
            data.append(self._get_row_data(election))
        return data

    def _get_row_data(self, election: soup):
        row = scraper_utils.initialize_row()
        text = election.text
        year_match = re.search(r'[A-Z][a-z]+ \d{1,2}, \d{4}', text)
        if (year_match):
            location = text.split(',')[0]
            row.election_date = self._get_election_date(year_match.group())
            row.election_name = self._get_election_name(location, row.election_date)
        else:
            location = text
            row.election_date = self._get_election_date_special(election)
            row.election_name = self._get_election_name(location, row.election_date)
        row.official_votes_record_url = self._get_ovr_url(election['href'])
        row.description = self._get_description(text, row.election_date, location)
        row.is_by_election = True
        return row

    def _get_election_date_special(self, election_link):
        url = MAIN_URL + election_link['href']
        page_soup = self.get_page_as_soup(url)
        main_content = page_soup.find('div', {'id':'content-main'})
        date = re.search(r'[A-Z][a-z]+ \d{1,2}, \d{4}', main_content.text)
        if date:
            return self._get_election_date(date.group())
        else:
            link = main_content.find_all(self._has_official_voting_result)[0]['href']
            page_soup = self.get_page_as_soup(MAIN_URL + link)
            date = re.search(r'[A-Z][a-z]+ \d{1,2}, \d{4}', page_soup.text)
            return self._get_election_date(date.group())

    def _get_election_date(self, text: str) -> str:
        date = datetime.datetime.strptime(text, '%B %d, %Y')
        date = date.strftime('%Y-%m-%d')
        return date

    def _get_election_name(self, location: str, date: str) -> str:
        name = date.replace('-', '_')
        location = location.replace('\x96', '-')
        location = location.replace(' ', '_').replace('(', '').replace(')', '').replace('–', '-')
        name = name + '_by_election_' + location
        return name

    def _get_description(self, text: str, date: str, location: str) -> str:
        location = location.replace('\x96', '-').replace('–', '-')
        if 'cancelled' in text.lower():
            return f"By-election which was held on {date} at {location} (cancelled)."
        return f"By-election which was held on {date} at {location}."

if __name__ == '__main__':
    program_driver()