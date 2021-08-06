import sys
import os
from pathlib import Path
import time
from selenium import webdriver
import scraper_utils
from scraper_utils import ElectorsScraperUtils
from database import CursorFromConnectionFromPool
from bs4 import BeautifulSoup
import pandas as pd
import dateutil.parser as dparser

NODES_TO_ROOT = 4
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

PATH = "../../../web_drivers/chrome_win_91.0.4472.19/chromedriver.exe"
browser = webdriver.Chrome(PATH)

#https://www.elections.ca/content.aspx?section=ele&dir=pas&document=index&lang=e
COUNTRY = 'ca'
TABLE = 'ca_electors'
MAIN_URL = 'https://www.elections.ca/'
ELECTIONS_URL = MAIN_URL + 'content.aspx?section=ele&dir=pas&document=index&lang=e'
THREADS_FOR_POOL = 12

scraper_utils = ElectorsScraperUtils(COUNTRY, TABLE)
crawl_delay = scraper_utils.get_crawl_delay(MAIN_URL)

with CursorFromConnectionFromPool() as cur:
    try:
        query = 'SELECT * FROM ca_elections'
        cur.execute(query)
        election = cur.fetchall()

        query = 'SELECT * FROM ca_electoral_districts'
        cur.execute(query)
        electoral_districts = cur.fetchall()

    except Exception as e:
        sys.exit(
            f'An exception occurred retrieving tables from database:\n{e}')

    elections = pd.DataFrame(election)
    districts = pd.DataFrame(electoral_districts)


def find_electors_links(link):
    page = scraper_utils.request(link)
    soup = BeautifulSoup(page.content, 'html.parser')
    main = soup.find('div', {'id': 'content-main'})
    voters_links = main.find_all('a')
    for link in voters_links:
        if "Voting" in link.text:
            voter_link = link.get('href')
            voter_link = MAIN_URL + voter_link
            return voter_link


def get_urls():
    urls = []
    election_links = []

    page = scraper_utils.request(ELECTIONS_URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    links_section = soup.find('div', {'id': 'content-main'})
    elections = links_section.find_all('a')
    for election in elections[2:]:
        link = election.get('href')
        link = MAIN_URL + link
        election_links.append(link)
    for link in election_links:
        link = find_electors_links(link)
        if link is not None:
            urls.append(link)

    return urls


def get_table_data(url):
    data = None
    browser.get(url)
    if browser.find_elements_by_tag_name('frame'):
        frames = (browser.find_elements_by_tag_name('frame'))
        browser.switch_to.frame(frames[0])
        input_tag = browser.find_element_by_tag_name("input")
        try:
            input_value = int(input_tag.get_attribute('value'))
        except:
            option_tag = browser.find_element_by_tag_name("option")
            input_value = int(option_tag.get_attribute('value'))
        url = url.replace('default.html', '')
        url = url.replace('home.html', '')
        data = get_election_with_frame(input_value, url)
    elements = browser.find_elements_by_tag_name('a')
    for e in elements:
        link = e.get_attribute('href')
        try:
            if '#3' in link:
                e.click()
            elif '#1' in link:
                e.click()
        except:
            pass
    links = browser.find_elements_by_tag_name('a')
    time.sleep(5)
    for l in links:
        try:
            if 'Number of electors and polling stations' in l.text:
                if 'for' not in l.text:
                    l.click()
                    data = get_election()
        except:
            pass
    try:
        if data == None:
            data = get_district_table()
    except Exception as e:
        print(e)

    if data is not None:
        row_list = []
        for i in data:
            province = i['province']
            if province == 'Newfoundland':
                province = 'Newfoundland and Labrador'
            try:
                province_id = int(get_prov_terr_id(province))
            except:
                try:
                    province_id = int(get_prov_terr_id_from_district(province))
                except:
                    province_id = int(0)

            try:
                election_id = get_election_id(i)
            except:
                election_id = int(0)

            if election_id != 0:
                if province_id != 0:
                    row_info = {'province_territory_id': province_id, 'election_id': election_id, 'electors': i['electors'], 'population': i['population']}

                    row_list.append(row_info)
        if row_list is None:
            row_list = []
        return row_list


def get_election_id(data):
    election = data['election']
    province = data['province']
    province = province.replace('–', '-')
    province = province.replace(' ', '_')
    if 'by-election' in election.lower():
        try:
            date = dparser.parse(election, fuzzy=True)
            date_name = date.strftime("%Y_%m_%d")
            election_name = date_name + '_by_election_' + province
        except Exception as e:
            print(e)
        if pd.notna(province):
            df = elections
            value = df.loc[(df['election_name'] == election_name)]['id'].values[0]
            try:
                return int(value)
            except Exception as e:
                return value
    if 'general' in election.lower():
        general_elections = {
            'fiftieth': '50th',
            'forty-ninth': '49th',
            'forty-eighth': '48th',
            'forty-seventh': '47th',
            'forty-sixth': '46th',
            'forty-fifth': '45th',
            'forty-fourth': '44th',
            'forty-third': '43rd',
            'forty-second': '42nd',
            'forty-first': '41st',
            'fortieth': '40th',
            'thirty-ninth': '39th',
            'thirty-eighth': '38th',
            'thirty-seventh': '37th',
            'thirty-sixth': '36th'
        }
        election = election.split(' ')[0].lower()
        e_number = general_elections.get(election)
        election_name = e_number + '_general_election'
        if pd.notna(province):
            df = elections
            value = df.loc[(df['election_name'] == election_name)]['id'].values[0]
            try:
                return int(value)
            except Exception as e:
                return value


def get_prov_terr_id_from_district(province):
    province = province.replace('–', '--')
    if pd.notna(province):
        df = districts
        value = df.loc[df["district_name"] == province]['province_territory_id'].values[0]
        try:
            return int(value)
        except Exception:
            return value


def get_prov_terr_id(province):
    if pd.notna(province):
        df = scraper_utils.divisions
        value = df.loc[df["division"] == province]['id'].values[0]
        try:
            return int(value)
        except Exception:
            return value


def get_election_with_frame(input_value, url):
    browser.get(url + f'{input_value}/table1.html')
    election_name =''
    try:
        election_name = browser.find_element_by_id('EventName')
    except:
        pass
    if election_name == '':
        election_name = browser.find_element_by_id('EventNameE')
    election_name = election_name.text
    return get_province_data_from_frame_table(election_name)


def get_election():
    try:
        election_name = browser.find_element_by_id('EventName')
        if election_name is None:
            election_name = browser.find_element_by_id('EventNameE')
        election_name = election_name.text
        return get_province_data(election_name)
    except:
        data = browser.find_elements_by_tag_name('b')
        if not data:
            data = get_district_table()
            return(data)
        for e in data:
            if 'election' in e.text:
                election_name = e.text
                election_name = election_name.split(':')[0]
                return get_province_data_from_alt_table(election_name)


def get_district_table():
    title = browser.find_element_by_tag_name('h1').text
    title = title.split(' – ')[0]
    return get_district_data(title)


def get_province_data_from_alt_table(election_name):
    provincial_data_list = []
    table = browser.find_elements_by_tag_name("tbody")[1]
    rows = table.find_elements_by_tag_name('tr')
    for r in rows[2:]:
        if 'Totals' not in r.text:
            items = r.find_elements_by_tag_name('td')
            province = items[0].text.strip()
            province = province.split('/')[0]
            population = items[1].text.replace(' ', '')
            population = population.replace(',', '')
            electors = items[2].text.replace(' ', '')
            electors = electors.replace(',', '')
            provincial_data = {'election': election_name, 'province': province, 'population': int(population), 'electors': int(electors)}
            provincial_data_list.append(provincial_data)
    return provincial_data_list


def get_province_data_from_frame_table(election_name):
    provincial_data_list = []
    table = browser.find_element_by_tag_name("tbody")
    rows = table.find_elements_by_tag_name('tr')
    for r in rows[8:]:
        if "Nunavut" in r.text:
            break
        else:
            items = r.find_elements_by_tag_name('td')
        province = items[0].text.strip()
        try:
            province = province.split('/')[0]
        except:
            pass
        population = items[1].text.replace(' ', '')
        population = population.replace(',', '')
        electors = items[2].text.replace(' ', '')
        electors = electors.replace(',', '')
        if province != '':
            provincial_data = {'election': election_name, 'province': province, 'population': int(population), 'electors': int(electors)}
            provincial_data_list.append(provincial_data)
    return provincial_data_list


def get_district_data(election):
    provincial_data_list = []
    tables = browser.find_elements_by_tag_name("table")
    for t in tables:
        if "Electors" in t.text:
            table = t
            rows = table.find_elements_by_tag_name('tr')
        elif "polling stations" in t.text:
            table = t
            rows = table.find_elements_by_tag_name('tr')

    for r in rows[1:]:
        if r.find_elements_by_tag_name('th'):
            items = r.find_elements_by_tag_name('td')
            try:
                district = r.find_element_by_tag_name('th').text
                pop = items[1].text.replace(',', '')
                pop = pop.replace(' ', '')
                electors = items[2].text.replace(',', '')
                electors = electors.replace(' ', '')
            except Exception as e:
                print(e)
        else:
            items = r.find_elements_by_tag_name('td')
            district = items[0].text
            pop = items[1].text.replace(',', '')
            pop = pop.replace(' ', '')
            electors = items[2].text.replace(',', '')
            electors = electors.replace(' ', '')
        if electors == '#':
            electors = 0
        if district != "Total":
            provincial_data = {'election': election, 'province': district,
                            'population': int(pop),
                            'electors': int(electors)}
            provincial_data_list.append(provincial_data)
    if not provincial_data_list:
        provincial_data_list.extend(get_paragraph_table(election))
    return provincial_data_list


def get_paragraph_table(election):
    provincial_data_list = []
    try:
        t_body = browser.find_elements_by_tag_name('tbody')[3]
        rows = t_body.find_elements_by_tag_name("td")
        districts = rows[1].text.split('\n')
        pop = rows[2].text.split('\n')
        electors = rows[3].text.split('\n')
    except:
        t_body = browser.find_elements_by_tag_name('tbody')[2]
        rows = t_body.find_elements_by_tag_name("td")
        if len(rows) > 2:
            districts = rows[1].text.split('\n')
            pop = rows[2].text.split('\n')
            electors = rows[3].text.split('\n')
        else:
            t_body = browser.find_element_by_tag_name('tbody')
            detail_row = t_body.find_elements_by_tag_name('tr')[2]
            list_rows = detail_row.find_elements_by_tag_name("td")
            districts = list_rows[9].text.split('\n')
            districts = districts[: districts.index(' ')]
            pop = list_rows[10].text.split('\n')
            pop = pop[:pop.index(' ')]
            electors = list_rows[11].text.split('\n')
            electors = electors[:electors.index(' ')]
    length = len(districts)
    for i in range(0, length):
        if districts[i] != 'Total':
            provincial_data = {'election': election, 'province': districts[i],
                           'population': int(pop[i].replace(' ', '')),
                           'electors': int(electors[i].replace(' ', ''))}
            provincial_data_list.append(provincial_data)
    return provincial_data_list


def get_province_data(election):
    provincial_data_list = []
    table = browser.find_element_by_tag_name("tbody")
    rows = table.find_elements_by_tag_name('tr')
    for r in rows:
        try:

            province = r.find_element_by_tag_name('th').text
            if "Totals" not in province:
                items = r.find_elements_by_tag_name('td')
                provincial_data = {'election': election, 'province': province,
                                    'population': int(items[0].text.replace(',', '')),
                                    'electors': int(items[1].text.replace(',', ''))}
                provincial_data_list.append(provincial_data)
        except Exception as e:
            print(e)

    return provincial_data_list


def get_row_data(data):
    row = scraper_utils.initialize_row()
    row.province_territory_id = int(data['province_territory_id'])
    row.election_id = int(data['election_id'])
    row.electors = int(data['electors'])
    row.population = int(data['population'])
    return row


if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')
    urls = get_urls()
    data = []
    data_list = []
    data.extend(get_table_data(url) for url in urls)

    lambda_obj = lambda x: (x is not None)

    list_out = list(filter(lambda_obj, data))

    flat_ls = [item for sublist in list_out for item in sublist]
    #print(data)
    # with Pool(processes=4) as pool:
    #     data = pool.map(scrape, urls)
    #print(data_list)
    row_data = [get_row_data(d) for d in flat_ls]
    scraper_utils.write_data(row_data)

    print('Complete!')


