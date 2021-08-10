# TODO: Write documentation for code.

import os
from pathlib import Path
import re
import sys
from time import sleep

from pandas.core.frame import DataFrame

NODES_TO_ROOT = 3
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from bs4 import BeautifulSoup as soup
from nameparser import HumanName
from numpy import nan
import pandas as pd
from scraper_utils import CandidatesScraperUtils
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

COUNTRY = 'ca'
CANDIDATES_BASE_URL = 'https://lop.parl.ca'
CANDIDATES_URL = CANDIDATES_BASE_URL + '/sites/ParlInfo/default/en_CA/ElectionsRidings/Elections'

scraper_utils = CandidatesScraperUtils(COUNTRY)
crawl_delay = scraper_utils.get_crawl_delay(CANDIDATES_BASE_URL)

options = Options()
options.headless = True

def program_driver():
    print("Collecting data...")
    scraper = Scraper()
    candidate_table_df = scraper.get_data()

    data_organizer = Organizer()
    data_organizer.set_rows(candidate_table_df)
    rows = data_organizer.get_rows()
    scraper_utils.write_data(rows)
    
    print("complete")

class Scraper:
    def __init__(self):
        self.driver = SeleniumDriver()
        self.driver.start_driver(CANDIDATES_URL, crawl_delay)

        try:
            self.data = self._get_candidate_data()
        except Exception as e:
            print(e.with_traceback())

        self.driver.close_driver()
        
    def get_data(self):
        return self.data

    def _get_candidate_data(self):
        self._prepare_page_for_collection()
        return self._collect_data()
    
    def _prepare_page_for_collection(self):
        self._expand_all_entries()
        # self._view_1000_entries()

    def _expand_all_entries(self):
        expand_all_button = self.driver.driver.find_element_by_css_selector('#gridContainer > div > div.dx-datagrid-header-panel > div > div > div.dx-toolbar-after > div:nth-child(2) > div > div > div')
        expand_all_button.click()
        sleep(10)

    # def _view_1000_entries(self):
    #     view_1000_div = self.driver.driver.find_elements_by_class_name('dx-page-size')[-1]
    #     view_1000_div.click()
    #     sleep(20)
    #     self.driver.driver.find_element_by_tag_name('html').send_keys('Keys.END')

    def _collect_data(self):
        data = DataFrame()
        while self._get_next_page_button():
        # for i in range(0,1):
            data = data.append(self._get_page_as_df())
            self._get_next_page_button().click()
            sleep(1)
        return data

    def _get_next_page_button(self):
        next_page_button = self.driver.driver.find_elements_by_class_name('dx-navigate-button')[-1]
        if "dx-button-disable" not in next_page_button.get_attribute('class'):
            return next_page_button

    def _get_page_as_df(self):
        html = self.driver.get_html_source()
        html_soup = soup(html, 'html.parser')
        html_table_data = html_soup.find_all('table', {'class': 'dx-datagrid-table dx-datagrid-table-fixed'})[1]
        df = pd.read_html(str(html_table_data), index_col=[0, 1, 2, 3])[0]
        df.columns = ['Province or Territory', 'Constituency', 'Candidate', 'Gender', 'Occupation', 'Political Affiliation', 'Result', 'Votes']
        df['Image URL'] = None
        self._get_candidate_images(html_soup, df)

        return df

    def _get_candidate_images(self, html_soup: soup, df):
        trs_with_images = html_soup.find_all('tr', {'class':'dx-row dx-data-row dx-column-lines'})
        for tr in trs_with_images:
            name = tr.find_all('td')[6].text

            img_url = tr.find('img')
            if img_url is not None:
                img_url = img_url.get('src')
                img_url = CANDIDATES_BASE_URL + img_url

            candidate_url = tr.find_all('td')[6].a
            if candidate_url is not None:
                candidate_url = candidate_url['href']
                candidate_url = CANDIDATES_BASE_URL + candidate_url
                img_url = self._get_img_url_from_candidate_profile(candidate_url)

            df.loc[df["Candidate"] == name, ['Image URL']] = img_url

    def _get_img_url_from_candidate_profile(self, url):
        self.driver.driver.execute_script(f'''window.open("{url}", "_blank");''')
        sleep(3.5)
        self.driver.tabs += 1
        self.driver.driver.switch_to_window(self.driver.driver.window_handles[self.driver.tabs - 1])
        image_div = self.driver.driver.find_element_by_id('PersonPic')
        img_url = image_div.find_element_by_tag_name('img').get_attribute('src')
        self.driver.driver.close()
        sleep(0.5)
        self.driver.tabs -= 1
        self.driver.driver.switch_to_window(self.driver.driver.window_handles[self.driver.tabs - 1])
        sleep(1)
        return img_url

class SeleniumDriver:
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
        sleep(10)

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

class Organizer:
    def __init__(self):
        self.rows = []
        self.new_entries = []
        self.checked_list = {}
        self.legislators_df = scraper_utils.legislators
        self.ed_df = scraper_utils.electoral_districts
        self.parties_df = scraper_utils.parties
        self.value_getter = DataValue()

    def get_rows(self):
        return self.rows

    def set_rows(self, df: DataFrame):
        for index, row in df.iterrows():
            value = row['Province or Territory']
            date_of_election = None
            if not ((pd.isna(value)) or 'Continued from'in value or 'Continues on' in value or 'Date of Election' in value or 
                        'Type of Election' in value or 'Parliament' in value):
                self._add_row_data(row, date_of_election)
            elif pd.notna(value) and 'Date of Election' in value:
                date_of_election = re.search(r'[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}', value).group()

    def _add_row_data(self, data_row, election_date):
        row = scraper_utils.initialize_row()
        row.current_electoral_district_id = self._get_district_id(data_row)
        self._set_name_data(row, data_row)
        row.gender = self._get_gender(data_row)
        row.current_party_id = self._get_party_id(data_row)
        row.candidate_image = self._get_image_url(data_row)
        row.goverlytics_id = self.get_goverlytics_id(row, election_date)
        if row.goverlytics_id:
            self.rows.append(row)

    def _get_district_id(self, data_row):
        district_name = data_row['Constituency']
        df = scraper_utils.electoral_districts
        district_id = self.value_getter.get_value(df, 'district_name', district_name, 'id')
        return int(district_id)
    
    def _set_name_data(self, row, data_row):
        name = data_row['Candidate']
        human_name = HumanName(name)
        row.name_full = human_name.full_name
        row.name_last = human_name.last
        row.name_first = human_name.first
        row.name_middle = human_name.middle
        row.name_suffix = human_name.suffix

    def _get_gender(self, data_row):
        gender = data_row['Gender']
        if pd.isna(gender) or gender is None:
            return ''
        elif gender == 'Man':
            return 'M'
        elif gender =='Woman':
            return 'F'
        else:
            return 'O'

    def _get_party_id(self, data_row):        
        df = scraper_utils.parties
        party_name = self._get_party_name(data_row)
        try:
            party_id = self.value_getter.get_value(df, 'party', party_name, 'id')
        except:
            party_id = self.value_getter.get_value(df, 'party', 'Other', 'id')
            if party_name != 'Unknown' and 'No affiliation' not in party_name:
                print(f"Party: {data_row['Political Affiliation']}")
        return int(party_id)

    def _get_party_name(self, data_row):
        party_name_flags = ['Canada', 'Canadian', 'Canada\'s']

        party_name = data_row['Political Affiliation']
        party_name = party_name.split(' (')[0]

        if party_name == 'Canada Party' or party_name == 'Canadian Party':
            return party_name
        if party_name.split(' ')[0] in party_name_flags:
            remove = party_name.split(' ')[0]
            return party_name.split(remove)[1].split(' Party')[0].strip()
        return party_name.split(' Party')[0]

    def _get_image_url(self, data_row):
        image_url = data_row['Image URL']
        if pd.isna(image_url) or image_url is None:
            return ''
        return image_url

    def _get_full_name(self, row):
        name_last = row.name_last
        name_first = row.name_first
        return name_first + ' ' + name_last

    def get_goverlytics_id(self, row, election_date):
        name_full = self._get_full_name(row)
        
        if name_full in self.checked_list and self._is_candidate_checked(row):
            return None

        name_match_df = self._get_legislator_name_match_df(row)

        if name_match_df.empty:
            return self._add_new_candidate_to_checked_list(row, election_date)

        if len(name_match_df) > 1:
            return self._handle_multiple_instances_of_candidate(row, name_match_df, election_date)

        return self._handle_single_instance_of_candidate(row, name_match_df, election_date)

    def _is_candidate_checked(self, row):
        print("This candidate seems to have been added already.")
        Printer().print_candidate_info(row)
        print(f"======================\n" +
              f"=====CHECKED_LIST=====\n" +
              f"======================\n")

        potential_candidate = self.checked_list.get(row.name_full)
        for potential in potential_candidate:
            party = potential.get("party")
            riding = potential.get('electoral_district')
            print(f"Party: {party}\n" +
                  f"Riding: {riding}\n")

        while True:
            user_input = input("Is the candidate listed above? y/n: ")                
            if user_input == 'y':
                return True
            elif user_input == 'n':
                return False
            else:
                print("Please enter y or n")

    def _get_legislator_name_match_df(self, row) -> DataFrame:
        name_last = row.name_last
        name_first = row.name_first
        name_last_match = self.legislators_df["name_last"].apply(str.lower) == name_last
        name_first_match = self.legislators_df["name_first"].apply(str.lower) == name_first
        return self.legislators_df.loc[(name_last_match) & (name_first_match)]

    def _handle_multiple_instances_of_candidate(self, row, name_match_df: DataFrame, election_date):
        Printer().print_candidate_info(row)
        print("More than one instance of the candidate with the same name was found in the database:")
        Printer().print_candidates_from_df(name_match_df)
        user_input = None
        while user_input is None:
            user_input_gov_id = input("Enter the goverlytic_id of correct candidate. If none, please enter 'None': ")
            user_input = self._get_user_input_for_multiple_candidate_instances(user_input_gov_id, name_match_df)
        if user_input == "EXISTS":
            return self._add_existing_candidate_to_checked_list(name_match_df, int(user_input_gov_id), election_date)
        elif user_input == "NEW":
            return self._add_new_candidate_to_checked_list(row, election_date)

    def _get_user_input_for_multiple_candidate_instances(self, user_input_gov_id, name_match_df: DataFrame):
        possible_gov_ids = name_match_df['goverlytics_id'].values

        if user_input_gov_id == "None" or (user_input_gov_id.isnumeric() and int(user_input_gov_id) in possible_gov_ids):
            while True:
                user_input = input(f"You entered: {user_input_gov_id}. Is this correct? (y/n): ")
                if user_input == 'y' and user_input_gov_id != "None":
                    return "EXISTS"
                elif user_input == 'y' and user_input_gov_id == "None":
                    return "NEW"
                elif user_input == 'n':
                    print("Try again.")
                    return
                else:
                    print("Please enter y or n")
        else:
            print("Make sure your input matches one of the goveryltic_ids or is None.")

    def _handle_single_instance_of_candidate(self, row, name_match_df: DataFrame, election_date):
        row_party_id = row.current_party_id
        match_party_id = name_match_df['party_id'].values[0]
        if int(match_party_id) != int(row_party_id):
            return self._get_user_input_for_candidate_info_mismatch("'party'", row, name_match_df, election_date)

        row_ed_id = row.current_electoral_district_id
        match_district = name_match_df['riding'].values[0]
        match_district = match_district.replace('—', '--')
        ed = self.value_getter.get_value(self.ed_df, 'id', row_ed_id, 'district_name')
        if ed != match_district:
            return self._get_user_input_for_candidate_info_mismatch("'district'", row, name_match_df, election_date)

        gov_id = name_match_df['goverlytics_id'].values[0]
        return self._add_existing_candidate_to_checked_list(name_match_df, gov_id, election_date)

    def _get_user_input_for_candidate_info_mismatch(self, mismatch_column, row, name_match_df, election_date):
        print(f"Candidate may already exist in the database, but the {mismatch_column} does not match.")
        print(f"Please check if candidate [{row.name_full}] is the same based on the information below:")
        Printer().print_candidate_info(row)
        Printer().print_legislator_info(name_match_df)

        while True:
            user_input = input("Are these the same individual? y/n: ")
            if user_input == 'y':
                gov_id =  name_match_df['goverlytics_id'].values[0]
                return self._add_existing_candidate_to_checked_list(name_match_df, gov_id, election_date)
            elif user_input == 'n':
                return self._add_new_candidate_to_checked_list(row, election_date)
            else:
                print("Please enter y or n")

    def _append_to_checked_list(self, name_full, gov_id, party, district, election_date):
        self.checked_list.setdefault(name_full, [])
        to_add = {
            'goverlytic_id': gov_id,
            'party': party,
            'electoral_district': district,
            'most_recent_election_date': election_date
        }
        self.checked_list.get(name_full).append(to_add)

    def _add_new_candidate_to_checked_list(self, row, election_date):
        name_full = self._get_full_name(row)
        ed_id = row.current_electoral_district_id
        party_id = row.current_party_id

        party = self.value_getter.get_value(self.parties_df, 'id', party_id, 'party')
        ed = self.value_getter.get_value(self.ed_df, 'id', ed_id, 'district_name')

        self._append_to_checked_list(name_full, None, party, ed, election_date)
        return -10

    def _add_existing_candidate_to_checked_list(self, name_match_df: DataFrame, gov_id, election_date):
        match_row = name_match_df.loc[name_match_df['goverlytics_id'] == gov_id]

        name_last = match_row['name_last'].values[0]
        name_first = match_row['name_first'].values[0]
        name_full = name_first + ' ' + name_last
        name_match_df_party_id = match_row['party_id'].values[0]
        name_match_df_party = self.value_getter.get_value(self.parties_df, 'id', name_match_df_party_id, 'party')
        name_match_df_riding = match_row['riding'].values[0]
        name_match_df_riding = name_match_df_riding.replace('—', '--')

        self._append_to_checked_list(name_full, gov_id, name_match_df_party, name_match_df_riding, election_date)
        return int(gov_id)

class DataValue:
    def get_value(self, df: DataFrame, search_column, search_value, return_column):
        return df.loc[df[search_column] == search_value][return_column].values[0]

class Printer:
    def print_candidates_from_df(self, df: DataFrame):
        for index, row in df.iterrows():
            goverlytics_id = row['goverlytics_id']
            party_id = row['party_id']
            party = DataValue().get_value(scraper_utils.parties, 'id', party_id, 'party')
            district = row['riding']
            district = district.replace('—', '--')
            print(f"goverlytics_id: {goverlytics_id}\n" +
                  f"party: {party} \n" +
                  f"district: {district}\n" +
                   "======================================")

    def print_candidate_info(self, row):
        party_id = row.current_party_id
        party = DataValue().get_value(scraper_utils.parties, 'id', party_id, 'party')
        ed_id = row.current_electoral_district_id
        district = DataValue().get_value(scraper_utils.electoral_districts, 'id', ed_id, 'district_name')
        self._print_info("==CANDIDATE INFO==", party, district, row.name_full)

    def print_legislator_info(self, name_match_df: DataFrame):
        party_id = name_match_df['party_id'].values[0]
        party = DataValue().get_value(scraper_utils.parties, 'id', party_id, 'party')
        district = name_match_df['riding'].values[0]
        district = district.replace('—', '--')
        self._print_info('LEGISLATOR DB INFO', party, district)

    def _print_info(self, label, party, district, candidate_name=None):
        print(f"======================\n" +
              f"=={label}==\n" +
              f"======================\n")
        if candidate_name:
            print(f"Name: {candidate_name}\n")
        print(f"Party: {party}\n" +
              f"District: {district}\n\n")

if __name__ == '__main__':
    program_driver()