import sys
import os
from pathlib import Path
from time import sleep

NODES_TO_ROOT = 3
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from scraper_utils import ElectionScraperUtils
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

COUNTRY = 'ca'
TABLE = 'ca_elections'
MAIN_URL = 'https://lop.parl.ca'
CANDIDATES_URL = MAIN_URL + '/sites/ParlInfo/default/en_CA/ElectionsRidings/Elections'

scraper_utils = ElectionScraperUtils(COUNTRY, TABLE)
crawl_delay = scraper_utils.get_crawl_delay(MAIN_URL)

options = Options()
options.headless = True

def program_driver():
    print("Collecting data...")
    scraper = Scraper()
    row_data = scraper.get_data()

    print("writing data...")
    scraper_utils.write_data(row_data)
    print("complete..")

class Scraper:
    def __init__(self):
        self.driver = SeleniumDriver()
        self.driver.start_driver(CANDIDATES_URL, crawl_delay)
        self.data = []

        try:
            self._get_election_data()
        except Exception as e:
            print(e.with_traceback())

        self.driver.close_driver()

    def get_data(self):
        return self.data

    def _get_election_data(self):
        self._prepare_page_for_collection()
        self._collect_data()

    def _prepare_page_for_collection(self):
        self._expand_all_entries()
        self._collaps_all_entries()
        self._view_1000_entries()
        try:
            self._expand_all_parliament()
        except IndexError as e:
            print("All elections expanded.")

    def _expand_all_entries(self):
        button = self.driver.driver.find_element_by_class_name('dx-icon.dx-icon-expand')
        button.click()
        sleep(1)

    def _collaps_all_entries(self):
        button = self.driver.driver.find_element_by_class_name('dx-icon.dx-icon-collapse')
        button.click()
        sleep(1)

    def _view_1000_entries(self):
        page_size_container = self.driver.driver.find_element_by_class_name('dx-page-sizes')
        page_sizes = page_size_container.find_elements_by_tag_name('div')
        button = page_sizes[-1]
        button.click()
        sleep(1)

    def _expand_all_parliament(self):
        button = self.driver.driver.find_elements_by_class_name('dx-command-expand.dx-datagrid-group-space.dx-datagrid-expand.dx-selection-disabled')
        
        index = 0
        number_buttons = len(self.driver.driver.find_elements_by_class_name('dx-command-expand.dx-datagrid-group-space.dx-datagrid-expand.dx-selection-disabled'))
        while index <= number_buttons:
            button = self.driver.driver.find_elements_by_class_name('dx-command-expand.dx-datagrid-group-space.dx-datagrid-expand.dx-selection-disabled')[index]
            aria_colindex = button.get_attribute('aria-colindex')
            if aria_colindex != '3':
                button.click()
                sleep(1)
            number_buttons = len(self.driver.driver.find_elements_by_class_name('dx-command-expand.dx-datagrid-group-space.dx-datagrid-expand.dx-selection-disabled'))
            index +=1

    def _collect_data(self):
        all_tds = self.driver.driver.find_elements_by_tag_name('td')

        type_of_election = ""
        parliament = ''
        for td in all_tds:
            if "Parliament: " in td.text:
                parliament = td.text.split(': ')[1]

            if "Type of Election: " in td.text:
                type_of_election = td.text.split(': ')[1]

            if "Date of Election: " in td.text:
                self._add_election_data(type_of_election, parliament, td)

    def _add_election_data(self, type_of_election, parliament, td_element):
        row = scraper_utils.initialize_row()
        row.election_date = self._get_election_date(td_element)
        row.is_by_election = self._set_by_election(type_of_election)
        row.official_votes_record_url = self._set_url(td_element)
        row.description = self._set_description(parliament, row)
        row.election_name = self._set_election_name(parliament, row)
        self.data.append(row)

    def _get_election_date(self, td_element):
        election_date = td_element.text.split(': ')[1].split(' Profile')[0]
        return election_date.strip()

    def _set_by_election(self, election_type):
        if election_type == 'General':
            return False
        elif election_type == 'By-Election':
            return True

    def _set_url(self, td_element):
        link_button = td_element.find_element_by_tag_name('button')
        on_click_action = link_button.get_attribute('onclick')
        link = on_click_action.split('(\'')[1].split('\')')[0]
        return MAIN_URL + link

    def _set_description(self, parliament, row):
        if row.is_by_election:
            return f"By-Election held for parliament {parliament} on {row.election_date}."
        return f"General Election held for parliament {parliament} on {row.election_date}"

    def _set_election_name(self, parliament, row):
        name = parliament
        if row.is_by_election:
            name += "_by_election_" + row.election_date
        else:
            name += '_general_election_' + row.election_date
        return name.replace('-', '_')

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

if __name__ == '__main__':
    program_driver()