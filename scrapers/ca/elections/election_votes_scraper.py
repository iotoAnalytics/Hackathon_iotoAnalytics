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


def find_election_votes_links(link):
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
        link = find_election_votes_links(link)
        if link is not None:
            urls.append(link)

    return urls


def get_table_data(url):
    print(url)
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
            if 'Number of ballots' in l.text:
                if 'for' not in l.text:
                    l.click()
                    data = get_election()
        except:
            pass
    try:
        if data == None:
            pass
            data = get_district_table()
    except Exception as e:
        print(e)


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
    print("1")
    browser.get(url + f'{input_value}/table3.html')
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
    print("2")
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
                print(election_name)
                return get_province_data_from_alt_table(election_name)


def get_district_table():
    print("3")
    title = browser.find_element_by_tag_name('table').text
    title = title.split(':')[0]
    return get_district_data(title)


def get_province_data_from_alt_table(election_name):
    print("4")
    provincial_data_list = []
    table = browser.find_elements_by_tag_name("tbody")[1]
    rows = table.find_elements_by_tag_name('tr')
    for r in rows[2:]:
        if 'Totals' not in r.text:
            items = r.find_elements_by_tag_name('td')
            province = items[0].text.strip()
            province = province.split('/')[0]
            valid_ballots = items[3].text.replace(' ', '')
            valid_ballots = valid_ballots.replace(',', '')
            invalid_ballots = items[5].text.replace(' ', '')
            invalid_ballots = invalid_ballots.replace(',', '')
            total_ballots = items[7].text.replace(' ', '')
            total_ballots = total_ballots.replace(',', '')
            voter_turnout = items[8].text.split('.')[0]

            provincial_data = {'election': election_name, 'province': province,
                                   'valid_ballots': valid_ballots,
                                   'invalid_ballots': invalid_ballots,
                                   'total_ballots': total_ballots,
                                   'voter_turnout': voter_turnout
                                   }
            provincial_data_list.append(provincial_data)
    print(provincial_data_list)
    return provincial_data_list


def get_province_data_from_frame_table(election_name):
    print("5")
    provincial_data_list = []
    table = browser.find_element_by_tag_name("tbody")
    rows = table.find_elements_by_tag_name('tr')
    for r in rows[8:-3]:
        if "Nunavut" in r.text:
            break
        else:
            items = r.find_elements_by_tag_name('td')
            c = 0
            # for i in items:
                # print(c)
                # print(i.text)
                # c+=1
        province = items[0].text.strip()
        try:
            province = province.split('/')[0]
        except:
            pass
        try:
            valid_ballots = items[3].text.replace(' ', '')
            valid_ballots = valid_ballots.replace(',', '')
            invalid_ballots = items[5].text.replace(' ', '')
            invalid_ballots = invalid_ballots.replace(',', '')
            total_ballots = items[7].text.replace(' ', '')
            total_ballots = total_ballots.replace(',', '')
            voter_turnout = items[8].text.split('.')[0]
        except:
            pass
        if province != '':
            provincial_data = {'election': election_name, 'province': province,
                               'valid_ballots': valid_ballots,
                               'invalid_ballots': invalid_ballots,
                               'total_ballots': total_ballots,
                               'voter_turnout': voter_turnout
                               }
            provincial_data_list.append(provincial_data)
    print(provincial_data_list)
    return provincial_data_list


def get_district_data(election):
    print("6")
    provincial_data_list = []
    districts = []
    tables = browser.find_elements_by_tag_name("table")
    for t in tables:
        if "Voting method" in t.text:
            table = t
            rows = table.find_elements_by_tag_name('tr')
        if "Electoral district" in t.text:
            table = t
            district_rows = table.find_elements_by_tag_name('tr')
            for r in district_rows[1:]:
                district_items = r.find_elements_by_tag_name('td')
                district = district_items[0].text
                districts.append(district)
    for r in rows:
        try:
            item = 1
            for d in districts:
                ordinary_polling_stations = rows[2].find_elements_by_tag_name('td')[item].text.replace(',', '')
                advance_polling_stations = rows[3].find_elements_by_tag_name('td')[item].text.replace(',', '')
                special_voting_rules = int(rows[4].find_elements_by_tag_name('td')[item].text.replace(',',''))
                special_voting_rules += int(rows[5].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                total = rows[6].find_elements_by_tag_name('td')[item].text
                item += 2
                voting_detail = {"district": d,
                             "ordinary_polling_stations": ordinary_polling_stations,
                             "advance_polling_stations": advance_polling_stations,
                             "special_voting_rules": special_voting_rules,
                             "total": total}
                provincial_data_list.append(voting_detail)
        except:
            item = 0
            for d in districts:
                ordinary_polling_stations = rows[2].find_elements_by_tag_name('td')[item].text.replace(',', '')
                advance_polling_stations = rows[3].find_elements_by_tag_name('td')[item].text.replace(',', '')
                special_voting_rules = int(rows[4].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                special_voting_rules += int(rows[5].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                total = rows[6].find_elements_by_tag_name('td')[item].text
                item += 2
                voting_detail = {"district": d,
                         "ordinary_polling_stations": ordinary_polling_stations,
                         "advance_polling_stations": advance_polling_stations,
                         "special_voting_rules": special_voting_rules,
                         "total": total}
                provincial_data_list.append(voting_detail)

    if not provincial_data_list:
        provincial_data_list = get_data_from_nested_tables(election)
    print(provincial_data_list)
    return(provincial_data_list)


def get_data_from_nested_tables(election):
    provincial_data_list = []
    outside_table = browser.find_element_by_tag_name('table')
    table = outside_table.find_element_by_tag_name('table')
    data_row = table.find_elements_by_tag_name('tr')[2]
    #print(data_row.text)
    columns = data_row.find_elements_by_tag_name('td')
    provinces = columns[0].text.split('\n')
    invalid_ballots = columns[5].text.split('\n')
    total_ballots = columns[7].text.split('\n')
    voter_turnout = columns[8].text.split('\n')

    # print(provinces)
    # print(invalid_ballots)
    # print(total_ballots)
    # print(voter_turnout)
    for i in range(0, 12):
        provincial_data = {'election': "election", 'province': provinces[i],

                           'invalid_ballots': invalid_ballots[i].replace(' ', ''),
                           'total_ballots': total_ballots[i].replace(' ', ''),
                           'voter_turnout': voter_turnout[i].split('.')[0]
                           }
        provincial_data_list.append(provincial_data)
    return provincial_data_list
    #     if r.find_elements_by_tag_name('th'):
    #         items = r.find_elements_by_tag_name('td')
    #         try:
    #             district = r.find_element_by_tag_name('th').text
    #             pop = items[3].text.replace(',', '')
    #             pop = pop.replace(' ', '')
    #             electors = items[4].text.replace(',', '')
    #             electors = electors.replace(' ', '')
    #         except Exception as e:
    #             print(e)
    #     else:
    #         items = r.find_elements_by_tag_name('td')
    #         district = items[0].text
    #         pop = items[3].text.replace(',', '')
    #         pop = pop.replace(' ', '')
    #         electors = items[4].text.replace(',', '')
    #         electors = electors.replace(' ', '')
    #     if electors == '#':
    #         electors = 0
    #     if district != "Total":
    #         provincial_data = {'election': election, 'province': district,
    #                         'population': pop,
    #                         'electors': electors}
    #         # provincial_data = {'election': election, 'province': province,
    #         #                    'valid_ballots': items[2].text.replace(',', ''),
    #         #                    'invalid_ballots': items[4].text.replace(',', ''),
    #         #                    'total_ballots': items[6].text.replace(',', ''),
    #         #                    'voter_turnout': items[7].text.replace(',', '')
    #         #                    }
    #         provincial_data_list.append(provincial_data)
    # if not provincial_data_list:
    #     provincial_data_list.extend(get_paragraph_table(election))
    # print(provincial_data_list)
    # return provincial_data_list


def get_paragraph_table(election):
    print("7")
    provincial_data_list = []
    try:
        t_body = browser.find_elements_by_tag_name('tbody')[3]
        rows = t_body.find_elements_by_tag_name("td")
        districts = rows[1].text.split('\n')
        pop = rows[4].text.split('\n')
        electors = rows[5].text.split('\n')
    except:
        t_body = browser.find_elements_by_tag_name('tbody')[2]
        rows = t_body.find_elements_by_tag_name("td")
        if len(rows) > 2:
            districts = rows[1].text.split('\n')
            pop = rows[4].text.split('\n')
            electors = rows[5].text.split('\n')
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
            # provincial_data = {'election': election, 'province': province,
            #                    'valid_ballots': items[2].text.replace(',', ''),
            #                    'invalid_ballots': items[4].text.replace(',', ''),
            #                    'total_ballots': items[6].text.replace(',', ''),
            #                    'voter_turnout': items[7].text.replace(',', '')
            #                    }
            provincial_data = {'election': election, 'province': districts[i],
                           'population': pop[i].replace(' ', ''),
                           'electors': electors[i].replace(' ', '')}
            provincial_data_list.append(provincial_data)
    print(provincial_data_list)
    return provincial_data_list


def get_province_data(election):
    print("8")
    provincial_data_list = []
    table = browser.find_element_by_tag_name("tbody")
    rows = table.find_elements_by_tag_name('tr')
    for r in rows:
        try:

            province = r.find_element_by_tag_name('th').text
            if "Totals" not in province:
                items = r.find_elements_by_tag_name('td')
                provincial_data = {'election': election, 'province': province,
                                    'valid_ballots': items[2].text.replace(',', ''),
                                    'invalid_ballots': items[4].text.replace(',', ''),
                                   'total_ballots': items[6].text.replace(',', ''),
                                   'voter_turnout': items[7].text.split('.')[0]
                                   }
                provincial_data_list.append(provincial_data)
        except Exception as e:
            print(e)
    print(provincial_data_list)
    return provincial_data_list


def get_row_data(data):
    row = scraper_utils.initialize_row()
    row.province_territory_id = int(data['province_territory_id'])
    row.election_id = int(data['election_id'])
    row.ordinary_stationary = int(data['electors'])
    row.ordinary_mobile = int(data['population'])
    row.advanced_polling = int(0)
    row.special_voting_rules = int(0)
    row.invalid_votes = int(0)
    row.voter_turnout = int(0)
    row.total = int(0)
    return row


if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')
    urls = get_urls()
    data = []
    data_list = []
    #data.extend(get_table_data(url) for url in urls)

    get_table_data('https://www.elections.ca//content.aspx?section=res&dir=rep/off/ovr_2013b2&document=index&lang=e')
    # lambda_obj = lambda x: (x is not None)
    #
    # list_out = list(filter(lambda_obj, data))
    #
    # flat_ls = [item for sublist in list_out for item in sublist]
    #print(data)
    # with Pool(processes=4) as pool:
    #     data = pool.map(scrape, urls)
    #print(data_list)
   # row_data = [get_row_data(d) for d in flat_ls]
   # scraper_utils.write_data(row_data)

    print('Complete!')
