"""
This program adds candidate election details for all historical elections in Canada.
Election data is collected from two different pages, and there are some discrepancies 
with names and data, requiring manual checking to be done.

This program should not be run again.

For version 2.0
-------------------
Version 2.0 should be made for subsequent elections so that new candidate election details can be added to the database.
This  would also require manual checking to be done, but will take less time to scrape. 
Version 2.0 should be run every time there is a new election.

Details
----------
Author: Kevin Nha
Version: 1.0
Date: 2021-09-15
"""

import os
from pathlib import Path
import sys
from time import sleep

NODES_TO_ROOT = 3
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from bs4 import BeautifulSoup as soup
from nameparser import HumanName
import pandas as pd
from pandas.core.frame import DataFrame
from scraper_utils import CandidatesElectionDetails
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

COUNTRY = 'ca'
BASE_URL = 'https://lop.parl.ca'
INCUMBENT_INFO_URL = BASE_URL + '/sites/ParlInfo/default/en_CA/ElectionsRidings/incumbentsRan'

scraper_utils = CandidatesElectionDetails(COUNTRY)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

options = Options()
options.headless = True

def program_driver():
    print("Collecting data...")
    election_id_and_links = get_election_links()

    file = File('scrapers/ca/elections/candidate_election_progress.txt')
    for id_and_link in election_id_and_links:
        data = get_data_from_election_link(id_and_link, file)
        scraper_utils.write_data(data)
        file.write_to_file(id_and_link[0])

def get_election_links() -> list:
    elections_df = scraper_utils.elections
    id_and_links = elections_df.loc[elections_df['id'] > 0][['id', 'official_votes_record_url']].values
    return [(id, link) for id, link in id_and_links]

def get_data_from_election_link(id_and_link, file):
    current_progress = file.read_file()

    election_id = str(id_and_link[0])
    if int(election_id) > int(current_progress):
        print(f"Currently on election ID: {election_id}")
        return collect_election_data(id_and_link)

def collect_election_data(id_and_link):
    election_data_scraper = Election(id_and_link)
    return election_data_scraper.get_rows()

class File:
    '''
    Class is used to read and write to files.
    '''
    def __init__(self, file_name):
        self.file_name = file_name

    def read_file(self):
        f = open(self.file_name, 'r')
        content = f.read()
        f.close()
        return content

    def write_to_file(self, content):
        f = open(self.file_name, 'w')
        f.write(content)
        f.close()

class Election:
    '''
    Collects and sets data for elections. 

    Params
    -----------
    id_and_link:    a tuple containing the election id and the link to the election
    '''
    def __init__(self, id_and_link):
        self.id_and_link = id_and_link
        self.election_id = int(self.id_and_link[0])
        self.parliament_number = self._get_parliament_number()
        self.driver = SeleniumDriver()
        
        self.incumbent_df = self._get_incument_data()
        self.df = self._get_data_df()
        self.rows = []
        self._set_rows()

        self.driver.close_driver()

    def get_rows(self) -> list:
        '''
        Returns the rows for the election.
        '''
        return self.rows

    def _get_parliament_number(self) -> int:
        elections_df = scraper_utils.elections
        election_name = elections_df.loc[elections_df['id'] == self.election_id]['election_name'].values[0]
        return int(str(election_name).split('_')[0])

    def _get_incument_data(self) -> pd.DataFrame:
        '''
        Collects incumbent data and returns it as a Dataframe.

        Returns
        ---------
        incumbent_df: DataFrame containing incumbent data
        '''
        if int(self.parliament_number) == 1:
            return pd.DataFrame()

        incumbent_driver = SeleniumDriver()
        incumbent_driver.start_driver(INCUMBENT_INFO_URL, crawl_delay)
        sleep(20)

        try_count = 5
        while try_count > 0:
            try:
                page_sizes = incumbent_driver.driver.find_elements_by_class_name('dx-page-size')
                page_sizes[-1].click()
                break
            except:
                sleep(2)
                try_count -= 1
        sleep(2)

        trs = incumbent_driver.driver.find_elements_by_tag_name('tr')
        tr = self._find_right_parliament_row(trs)

        try:
            expand_button = tr.find_element_by_class_name('dx-datagrid-group-closed')
            expand_button.click()
            sleep(3)

        except:
            print(f"No expand button found for election id: {self.election_id}")

        html = incumbent_driver.get_html_source()
        html_soup = soup(html, 'html.parser')
        html_table_data = html_soup.find_all('table', {'class': 'dx-datagrid-table dx-datagrid-table-fixed'})[1]
        df = pd.read_html(str(html_table_data), index_col=[0])[0]
        df.columns = ['Name', 'Constituency', 'Province/Territory', 'Political Affiliation', 'Date of Next Election', 'Constituency at Next Election', 'Province/Territory at Next Election', 'Political Affiliation at Next Election', 'Result at Next Election']
        df['name_last'] = df.apply(lambda row: HumanName(row.Name).last, axis=1)

        incumbent_driver.close_driver()
        return df

    def _find_right_parliament_row(self, trs):
        '''
        Returns the correct table row that has incumbent data for the particular parliament number

        Returns
        ---------
        tr: table_row (a Selenium webelement) that corresponds to the incumbent data for the parliament number
        '''
        parliament_number_to_look_for = self._get_search_parliament_number()

        for tr in trs:
            if str(parliament_number_to_look_for) in tr.text:
                return tr

    def _get_search_parliament_number(self) -> int:
        return self.parliament_number - 1

    def _get_data_df(self) -> pd.DataFrame:
        '''
        Gets the candidate data from the election URL and returns it as a DataFrame.

        Returns
        ----------
        df: DataFrame containing candidate data from election URL
        '''
        self._open_election_link()
        self._prepare_site_for_collection()
        df = self._get_data_from_link()
        while self._get_next_page_button():
            click_count_try = 5
            while click_count_try > 0:
                try:
                    self._get_next_page_button().click()
                    sleep(4)
                    break
                except:
                    sleep(2)
                    click_count_try -= 1
            df = df.append(self._get_data_from_link())
        return df

    def _get_data_from_link(self) -> pd.DataFrame:
        '''
        Collects the data on the election URL.

        Table columns need to be defined because pd.read_html() does not read the table on the page correctly without speciyfing.

        Returns
        -----------
        df: DataFrame with candidate details for the election
        '''

        page_source_html = self.driver.get_html_source()
        page_soup = soup(page_source_html, 'html.parser')
        table = page_soup.find_all('table', {'class':'dx-datagrid-table dx-datagrid-table-fixed'})[-1]
        df = pd.read_html(str(table), index_col=[0, 1])[0]
        df.columns = ['Candidate', 'Political Party', 'Gender', 'Occupation', 'Result', 'Votes']
        return df

    def _open_election_link(self):
        link = self.id_and_link[1]
        self.driver.start_driver(link, crawl_delay)

    def _prepare_site_for_collection(self):
        '''
        Expands the contents on the page and dispalys 1000 items per page, when possible
        '''
        self._expand_all()
        try:
            self._view_1000_items()
        except:
            print(f"No 1000 items button for election_id: {self.election_id}")
        
    def _expand_all(self):
        '''
        Expands the contents of the page
        '''
        expand_all_checkbox = self.driver.driver.find_element_by_class_name('dx-checkbox-icon')
        expand_all_checkbox.click()
        sleep(3)

    def _view_1000_items(self):
        '''
        Clicks the display 1000 items button
        '''
        page_sizes = self.driver.driver.find_elements_by_class_name('dx-page-size')
        page_sizes[-1].click()
        sleep(3)

    def _get_next_page_button(self):
        '''
        Returns the div that contains the button for going to the next page

        Returns
        ---------
        next_page_button: div containing the next page button
        '''
        try:
            next_page_button = self.driver.driver.find_elements_by_class_name('dx-navigate-button')[-1]
        except:
            return False
        if "dx-button-disable" not in next_page_button.get_attribute('class'):
            return next_page_button

    def _set_rows(self):
        '''
        Sets row data by appending each row to self.rows
        '''
        electoral_district = None
        count = 0
        for index, row in self.df.iterrows():
            value = row['Candidate']
            if pd.notna(value) and 'Constituency' in value:
                electoral_district = str(value).split(': ')[1].split(' (Contin')[0]
            elif pd.notna(value) and 'Province / Territory:' not in value:
                self.rows.append(self._set_row_data(row, electoral_district))
            count += 1

    def _set_row_data(self, table_row: DataFrame, electoral_district: str):
        '''
        Populates the row data.

        Returns
        -------------
        row: CandidateRow 
        '''
        row = scraper_utils.initialize_row()
        row.election_id = self.election_id
        row.electoral_district_id = self._get_electoral_district_id(electoral_district)
        row.party_id = self._get_party_id(table_row)
        row.is_incumbent = self._get_is_incumbent(table_row, electoral_district)
        row.candidate_id = self._get_candidate_id(table_row)
        return row

    def _get_electoral_district_id(self, electoral_district: str) -> int:
        '''
        Returns the electoral district id by comparing the electoral district name with the database

        Returns
        -----------
        id: electoral district id
        '''

        df = scraper_utils.electoral_districts
        id = df.loc[df['district_name'] == electoral_district]['id'].values[0]
        return int(id)

    def _get_party_id(self, table_row: pd.DataFrame) -> int:
        '''
        Returns the party id by comparing the party name with the database.

        Returns
        --------------
        id: party id. If party name does not exist in the database or candidate was not affiliated to any party, it will return the party id for "Other"
        '''
        party = self._get_party_name(table_row)
        df = scraper_utils.parties
        try:
            id = df.loc[df['party'] == party]['id'].values[0]
        except:
            id = df.loc[df['party'] == 'Other']['id'].values[0]
            if party != 'Unknown' and 'No affiliation' not in party:
                print(f"Party: {table_row['Political Party']} not found for {table_row['Candidate']}")
        return int(id)

    def _get_party_name(self, table_row: pd.DataFrame) -> str:
        '''
        Takes the party name that is on the election page and modifies the party name so it can be used to search the database.

        Returns:
        -----------
        party_name: Modified party name 
        '''
        party_name_flags = ['Canada', 'Canadian', 'Canada\'s']

        party_name = table_row['Political Party']
        party_name = party_name.split(' (')[0]

        if party_name == 'Canada Party' or party_name == 'Canadian Party':
            return party_name
        if party_name.split(' ')[0] in party_name_flags:
            remove = party_name.split(' ')[0]
            return party_name.split(remove)[1].split(' Party')[0].strip()
        return party_name.split(' Party')[0]

    def _get_candidate_id(self, table_row: pd.DataFrame) -> int:
        '''
        Returns the goverlytic id of the candidate.

        This function searches the candidate table in the database. It searches using the candidates last and first name.
        If there is a single match, it returns goverlytics id of the matched candidate.

        If there are multiple matches, or no matches, it requires a manual check to see which candidate is the correct entry in the database 
        of if the candidate really does not exist in the database.

        If the candidate does not exist in the database, you must manually add the candidate by running a query on PgAdmin on the candidate table.
        Then you must enter the goverlytic_id of the new candidate as the candidate_id

        Returns
        -----------
        candidate_id: The goverlytics id of the candidate
        '''
        candidate_name = table_row['Candidate']
        full_name = HumanName(candidate_name)

        name_match = self._get_match_from_candidate_df(full_name)
        possible_gov_ids = name_match['goverlytics_id'].values

        if len(name_match) > 1 or name_match.empty:
            print("Candidate info: ", full_name, "\nParliament Number: ", self.parliament_number)
            user_input = None
            while user_input is None:
                user_input_gov_id = input("Enter the goverlytic_id of correct candidate: ")
                if user_input_gov_id.isnumeric() and (len(possible_gov_ids) == 0 or int(user_input_gov_id) in possible_gov_ids):
                    while True:
                        user_input = input(f"You entered: {user_input_gov_id}. Is this correct? (y/n): ")
                        if user_input == 'y':
                            return int(user_input_gov_id)
                        elif user_input == 'n':
                            print("Try again.")
                            user_input = None
                            break
                        else:
                            print("Please enter y or n")
        return int(possible_gov_ids[0])

    def _get_match_from_candidate_df(self, name_full: HumanName) -> pd.DataFrame:
        '''
        Finds matches from the candidate table in the database by searching with the first and last name.

        Returns
        -----------
        name_match: DataFrame containing name matches from Candidate df
        '''
        name_last = name_full.last.lower()
        name_first = name_full.first.lower()

        df = scraper_utils.candidates
        name_last_match = df["name_last"].apply(str.lower) == name_last
        name_first_match = df["name_first"].apply(str.lower) == name_first

        name_match = df.loc[(name_last_match) & (name_first_match)]
        return name_match

    def _get_is_incumbent(self, table_row: pd.DataFrame, electoral_district: str) -> bool:
        '''
        Checks whether candidate is incumbent or not.

        Candidates from the first parliament will return False as there is no previous election.

        This function checks the last name of the candidate from the incumbent dataframe.
        If there is a match, it also checks the party, result, and riding. Having all these fields match will return True.

        Returns
        ---------
        True: if incumbent, else False.
        
        '''
        if int(self.parliament_number) == 1:
            return False

        candidate_name = str(table_row['Candidate']).strip().lower()
        name = HumanName(candidate_name)
        name_last = name.last

        name_last_match = self.incumbent_df["name_last"].apply(str.lower) == name_last.lower()
        
        name_match = self.incumbent_df.loc[name_last_match]
        if not name_match.empty:
            party_match = name_match.loc[name_match['Political Affiliation at Next Election'] == str(table_row['Political Party']).strip()]
            result_match = name_match.loc[name_match['Result at Next Election'] == str(table_row['Result']).strip()]
            electoral_match = name_match.loc[name_match['Constituency'] == electoral_district]
            if not (party_match.empty or result_match.empty or electoral_match.empty):
                return True
        return False

class SeleniumDriver:
    """
    Used to handle Selenium.
    """
    def __init__(self):
        self.driver = webdriver.Chrome('web_drivers/chrome_win_92.0.4515.43/chromedriver.exe', options=options)
        self.driver.switch_to.default_content()
        self.tabs = 0

    def start_driver(self, url, crawl_delay):
        try:
            self.tabs +=1
            self.driver.get(url)
            self.driver.maximize_window()
        except:
            self.tabs -=1
            self.close_driver()
            raise RuntimeError("could not start webdriver")
        scraper_utils.crawl_delay(crawl_delay)
        sleep(5)

    def close_driver(self):
        self.driver.close()
        self.tabs -=1
        self.driver.quit()

    def get_html_source(self):
        try:
            html = self.driver.page_source
            return html
        except:
            self.close_driver()
            raise RuntimeError("Error in getting email table from selenium.")

if __name__ == '__main__':
    program_driver()