from collections import OrderedDict
import os
from pathlib import Path
from platform import python_revision
import re
import sys
from time import sleep

NODES_TO_ROOT = 3
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from bs4 import BeautifulSoup as soup
from multiprocessing import Pool
import pandas as pd
from pandas.core.frame import DataFrame
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from scraper_utils import ElectoralDistrictScraperUtils
from urllib.request import urlopen

COUNTRY = 'ca'
TABLE = 'ca_electoral_districts'
RIDING_BASE_URL = 'https://lop.parl.ca'
RIDING_URL = RIDING_BASE_URL + '/sites/ParlInfo/default/en_CA/ElectionsRidings/Ridings'
ELECTIONS_BASE_URL = 'https://www.elections.ca'
NAME_CHANGE_POPULATION_URL = ELECTIONS_BASE_URL + '/content.aspx?section=res&dir=cir/list&document=index338&lang=e'
THREADS_FOR_POOL = 12

NAME_CHANGE_URLS = OrderedDict()
NAME_CHANGE_URLS[2016] = 'https://www.elections.ca/content.aspx?section=res&dir=cir/maps2/chang&document=index&lang=e'
NAME_CHANGE_URLS[2004] = 'https://www.elections.ca/content.aspx?section=res&dir=cir/list&document=index&lang=e'
NAME_CHANGE_URLS[2000] = 'https://www12.statcan.gc.ca/fedprofil/eng/FedNameChange_E.cfm'

DF_COLUMN_INDEX_KV = {
    "name": 0,
    "province": 1,
    "region": 2,
    "start_date": 3,
    "end_date": 4,
    "currently_active": 5
}

scraper_utils = ElectoralDistrictScraperUtils(COUNTRY, TABLE)

riding_crawl_delay = scraper_utils.get_crawl_delay(RIDING_BASE_URL)
elections_crawl_delay = scraper_utils.get_crawl_delay(ELECTIONS_BASE_URL)

options = Options()
options.headless = True

def program_driver():
    district_data = Districts().get_data()

class PreProgramFunction:
    def get_population_csv(self):
        page_html = self._get_site_as_html()
        page_soup = soup(page_html, 'html.parser')
        self.link_to_csv = ELECTIONS_BASE_URL + self._find_csv_link(page_soup)
        csv_df = self._open_csv_file()
        return csv_df
    def _get_site_as_html(self):
        uClient = urlopen(NAME_CHANGE_POPULATION_URL)
        page_html = uClient.read()
        uClient.close()
        scraper_utils.crawl_delay(elections_crawl_delay)
        return page_html

    def _find_csv_link(self, page_soup: soup) -> str:
        link = page_soup.find_all(self._has_CSV_in_link_text)[0]['href']
        return link

    def _has_CSV_in_link_text(self, tag: soup):
        return "CSV" in tag.text and tag.name == 'a'

    def _open_csv_file(self):
        return pd.read_csv(self.link_to_csv, encoding='latin-1')

    def get_census_year(self):
        return int(re.search(r'[0-9]{4}', self.link_to_csv).group())

class Districts:
    def __init__(self):
        self.driver_instance = SeleniumDriver()
        self.driver_instance.start_driver(RIDING_URL, riding_crawl_delay)
        try:
            self.data = self._get_district_data()
        except Exception as e:
            print(e.with_traceback())
            print(f"Error getting riding data")
        self.driver_instance.close_driver()

    def get_data(self):
        return self.data

    def _get_district_data(self) -> list:
        self._display_500_items()
        sleep(2)
        self._show_inactive_districts()
        sleep(2)
        data_df = self._get_data_as_df()
        self.previous_names_dictionary = self._get_prev_district_names()
        print(self.previous_names_dictionary)
        try:
            rows_data = self._get_rows_data(data_df)
        except Exception as e:
            print(e.with_traceback())

    def _display_500_items(self):
        display_500_items = self.driver_instance.driver.find_element_by_css_selector("div[aria-label='Display 500 items on page']")
        display_500_items.click()
        
    def _show_inactive_districts(self):
        show_inactive_checkbox = self.driver_instance.driver.find_elements_by_class_name('dx-checkbox-icon')[-1]
        show_inactive_checkbox.click()

    def _get_data_as_df(self) -> DataFrame:
        pages_container = self.driver_instance.driver.find_element_by_class_name('dx-pages')
        pages = pages_container.find_elements_by_class_name('dx-page')
        data = self._collect_data()
        for index in range(1, len(pages)):
            pages[index].click()
            sleep(2)
            pages_container = self.driver_instance.driver.find_element_by_class_name('dx-pages')
            pages = pages_container.find_elements_by_class_name('dx-page')
            data = data.append(self._collect_data())
        pages[0].click()
        sleep(2)
        return data

    def _collect_data(self):
        html = self.driver_instance.get_html_source()
        html_soup = soup(html, 'html.parser')
        html_data_table = html_soup.find('table', {'style':'table-layout: fixed;'})
        return pd.read_html(str(html_data_table))[0]

    def _get_prev_district_names(self):
        '''
        What I can try to do is recursively call a function
        I can continue looking for previous names until none shows up.
        whilst looking, I can update the districts I pass. 

        This function should return a dictionary: 
            Key = name
            Value = previous_names []
        
        
        '''
        pages_container = self.driver_instance.driver.find_element_by_class_name('dx-pages')
        pages = pages_container.find_elements_by_class_name('dx-page')
        data = []
        for index in range(len(pages)):
            pages[index].click()
            sleep(2)
            pages_container = self.driver_instance.driver.find_element_by_class_name('dx-pages')
            pages = pages_container.find_elements_by_class_name('dx-page')
            data.extend(self._find_previous_names())
        data_dictionary = {k:v for names in data for k, v in names.items()}
        self._update_data_dictinary(data_dictionary)
        return data_dictionary

    def _find_previous_names(self):
        table = self.driver_instance.driver.find_elements_by_tag_name('table')[1]
        links = table.find_elements_by_tag_name('a')
        links = [link.get_attribute('href') for link in links]
        return get_data_from_all_links(get_previous_data, links)

    def _update_data_dictinary(self, data_dictionary: dict):
        for key in data_dictionary.keys():
            for item in data_dictionary.get(key):
                if item in data_dictionary.keys():
                    data_dictionary.get(key).extend(data_dictionary.get(item))
        
        for key in data_dictionary.keys():
            data_dictionary[key] = list(dict.fromkeys(data_dictionary[key]))

    def _get_rows_data(self, df: DataFrame) -> list:
        return_data = []
        i = 0
        length = len(df)
        for index, row in df.iterrows():
            i += 1
            if i < length:
                return_data.append(self._add_row_data(row))
        return return_data

    def _add_row_data(self, data_row):
        row = scraper_utils.initialize_row()
        try:
            row.province_territory_id = self._get_prov_terr_id(data_row)
        except TypeError:
            pass
        row.district_name = self._get_district_name(data_row)
        row.region = self._get_region(data_row)
        row.is_active = self._get_active_status(data_row)
        row.start_date = self._get_start_date(data_row)
        row.population = self._get_population(row)
        if row.population:
            row.census_year = census_year
        row.prev_district_names = self.previous_names_dictionary.setdefault(row.district_name, [])
        return row

    def _get_prov_terr_id(self, data_row):
        province = data_row[DF_COLUMN_INDEX_KV.get('province')]
        if pd.notna(province):
            df = scraper_utils.divisions
            value = df.loc[df["division"] == province]['id'].values[0]
            try:
                return int(value)
            except Exception:
                return value

    def _get_district_name(self, data_row):
        district_name = data_row[DF_COLUMN_INDEX_KV.get('name')]
        if pd.notna(district_name):
            return district_name
        return ''

    def _get_region(self, data_row):
        region = data_row[DF_COLUMN_INDEX_KV.get('region')]
        if pd.notna(region):
            return region
        return ''

    def _get_active_status(self, data_row):
        status = data_row[DF_COLUMN_INDEX_KV.get('currently_active')].lower()
        if 'active' == status:
            return True
        return False

    def _get_start_date(self, data_row):
        start_date = data_row[DF_COLUMN_INDEX_KV.get('start_date')]
        if pd.notna(start_date):
            return start_date
        return ''

    def _get_population(self, row):
        name = row.district_name.replace('’', '\'').replace('--', '-')
        if row.is_active:
            try:
                value = population_df.loc[population_df["ED_NAMEE"].replace('--', '-') == name]['POPULATION'].values[0]
                return int(value)
            except:
                pass

class SeleniumDriver:
    def __init__(self):
        self.driver = webdriver.Chrome('web_drivers/chrome_win_92.0.4515.43/chromedriver.exe', options=options)
        self.driver.switch_to.default_content()
        self.tabs = 0

    def start_driver(self, url, crawl_delay):
        try:
            self.driver.get(url)
            self.driver.maximize_window()
        except:
            self.close_driver()
            raise RuntimeError("could not start webdriver")
        scraper_utils.crawl_delay(crawl_delay)
        sleep(10)

    def close_driver(self):
        self.driver.close()
        self.driver.quit()

    def get_html_source(self):
        try:
            html = self.driver.page_source
            return html
        except:
            self.close_driver()
            raise RuntimeError("Error in getting email table from selenium.")

class PreviousNameCollector:
    def __init__(self, url):
        self.driver =  SeleniumDriver()
        self.driver.start_driver(url, riding_crawl_delay)
        self.data = self._extract_previous()
        self.driver.close_driver()

    def _extract_previous(self):
        paragraph = self.driver.driver.find_element_by_id('RidingNotes').text
        if "amended by substituting the name" in paragraph:
            match = re.findall(r'\“(.*?)\”', paragraph)
            if match:
                name_current = match[0]
                name_previous = match[1]
                return {name_current: [name_previous]}
        return {}

    def get_previous_name_data(self):
        return self.data


def get_data_from_all_links(function, all_links):
    data = []
    with Pool() as pool:
        data = pool.map(func=function,
                        iterable=all_links)
    return data

def get_previous_data(link):
    name_collector = PreviousNameCollector(link)
    return name_collector.get_previous_name_data()

#global variable
PreProgram = PreProgramFunction()
population_df = PreProgram.get_population_csv()
census_year = PreProgram.get_census_year()

if __name__ == "__main__":
    program_driver()
    pass