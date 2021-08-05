import os
from pathlib import Path
import re
import sys
from time import sleep
import traceback

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
        # while self._get_next_page_button():
        for i in range(0,1):
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

    def set_rows(self, df: DataFrame):
        for index, row in df.iterrows():
            value = row['Province or Territory']
            date_of_election = None
            if not ((pd.isna(value)) or 'Continued from'in value or 'Continues on' in value or 'Date of Election' in value or 
                        'Type of Election' in value or 'Parliament' in value):
                self._add_row_data(row, date_of_election)
            elif pd.notna(value) and 'Date of Election' in value:
                date_of_election = re.search(r'[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}', value).group()

    def get_rows(self):
        return self.rows

    def _add_row_data(self, data_row, election_date):
        row = scraper_utils.initialize_row()
        row.current_electoral_district_id = self._get_district_id(data_row)
        self._set_name_data(row, data_row)
        row.gender = self._get_gender(data_row)
        row.current_party_id = self._get_party_id(data_row)
        row.candidate_image = self._get_image_url(data_row)
        row.goverlytics_id = self._get_goverlytics_id(row)

        self.rows.append(row)

    def _get_district_id(self, data_row):
        district_name = data_row['Constituency']
        df = scraper_utils.electoral_districts
        district_id = df.loc[df["district_name"] == district_name]['id'].values[0]
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
        party_name_flags = ['Canada', 'Canadian', 'Canada\'s']

        party_name = data_row['Political Affiliation']
        party_name = party_name.split(' (')[0]
        if party_name == 'Canada Party' or party_name == 'Canadian Party':
            pass
        elif party_name.split(' ')[0] in party_name_flags:
            remove = party_name.split(' ')[0]
            party_name = party_name.split(remove)[1].split(' Party')[0].strip()
        else:
            party_name = party_name.split(' Party')[0]
        
        df = scraper_utils.parties
        try:
            party_id = df.loc[df["party"] == party_name]['id'].values[0]
        except:
            party_id = df.loc[df["party"] == 'Other']['id'].values[0]
            if party_name != 'Unknown' and 'No affiliation' not in party_name:
                print(f"Party: {data_row['Political Affiliation']}")
        return int(party_id)

    def _get_image_url(self, data_row):
        image_url = data_row['Image URL']
        if pd.isna(image_url) or image_url is None:
            return ''
        return image_url

    def _get_goverlytics_id(self, row):
        legislators_df = scraper_utils.legislators
        ed_df = scraper_utils.electoral_districts

        name_last = row.name_last
        name_first = row.name_first
        ed_id = row.current_electoral_district_id
        party_id = row.current_party_id

        name_match_df = legislators_df.loc[(legislators_df["name_last"] == name_last) & (legislators_df["name_first"] == name_first)]
        
        '''
        TODO: Test Driven Development
        TODO: What is the name_match_df length is greater than 1?
        TODO: Need to add new candidates to a dataframe and check if it exists there too... then do the SAME thing below.
        '''
        
        if name_match_df.empty:
            return -10 # This is the flag to note that there is 
        name_match_df_party_id = name_match_df['party_id'].values[0]
        print(int(name_match_df_party_id), int(party_id))
        if int(name_match_df_party_id) != int(party_id):
            print("Candidate may already exist in the database, but the party does not match.")
            print(f"Please check if candidate [{row.name_full}] is the same based on the information below:")
            print(f"======================\n" +
                  f"==LEGISLATOR DB INFO==\n" +
                  f"======================\n" +
                  f"Party: {scraper_utils.parties[name_match_df['party_id']]}\n" +
                  f"Riding: {name_match_df['riding']}\n\n")
            print(f"======================\n" +
                  f"====CANDIDATE INFO====\n" +
                  f"======================\n" +
                  f"Party: {scraper_utils.parties[party_id]}\n" +
                  f"Riding: {ed_df['ed_id']}\n\n")
            
            while True:
                user_input = input("Are these the same individual? y\\n:")
                if user_input == 'y':
                    return name_match_df['goverlytics_id']
                elif user_input == 'y':
                    return -10
                else:
                    print("Please enter y or n")

        riding = name_match_df['riding'].values[0]
        riding = riding.replace('—', '--')
        ed = ed_df.loc[ed_df['id'] == ed_id]['district_name'].values[0]
        print(ed, riding)
        if ed != riding:
            print("Candidate may already exist in the database, but the riding does not match.")
            print(f"Please check if candidate [{row.name_full}] is the same based on the information below:")
            print(f"======================\n" +
                  f"==LEGISLATOR DB INFO==\n" +
                  f"======================\n" +
                  f"Party: {scraper_utils.parties[name_match_df['party_id']]}\n" +
                  f"Riding: {name_match_df['riding']}\n\n")
            print(f"======================\n" +
                  f"====CANDIDATE INFO====\n" +
                  f"======================\n" +
                  f"Party: {scraper_utils.parties[party_id]}\n" +
                  f"Riding: {ed_df['ed_id']}\n\n")

            while True:
                user_input = input("Are these the same individual? y\\n:")
                if user_input == 'y':
                    return name_match_df['goverlytics_id']
                elif user_input == 'y':
                    return -10
                else:
                    print("Please enter y or n")

        print(name_match_df['goverlytics_id'].values[0])
        return int(name_match_df['goverlytics_id'].values[0])

if __name__ == '__main__':
    program_driver()