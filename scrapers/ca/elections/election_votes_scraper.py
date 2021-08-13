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

# https://www.elections.ca/content.aspx?section=ele&dir=pas&document=index&lang=e
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
        # opening the table tab
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
            if 'Number of ballots cast, by voting method' in l.text:
                l.click()
                data = get_district_table()
            if 'Number of ballots' in l.text:
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
    print(data)


def get_second_alt_table(data):
    print("get_second_alt_table_data")
    browser.back()
    elements = browser.find_elements_by_tag_name('a')
    for e in elements:
        link = e.get_attribute('href')
        # opening the table tab
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
            if 'by voting method' in l.text:
                l.click()
                full_data = get_voting_methods_alt_table(data)
            return full_data
        except:
            pass


def get_voting_methods_alt_table(data):
    print('get_voting_methods_alt_table')
    full_data_list = []
    table = browser.find_elements_by_tag_name("tbody")[1]
    rows = table.find_elements_by_tag_name('tr')
    for r in rows[2:]:
        if 'Totals' not in r.text:
            items = r.find_elements_by_tag_name('td')
            province = items[0].text.strip()
            province = province.split('/')[0]
            ordinary = int(items[1].text.replace(' ', ''))
            ordinary += int(items[2].text.replace(' ', ''))
            advance = items[3].text.replace(' ', '')
            special = items[4].text.split('.')[0]

    for d in range(0, 13):
        if data[d]['province'] == province:
            full_data = {'election': data[d]['election'], 'province': province,
                         'invalid_ballots': data[d]['invalid_ballots'],
                         'total': data[d]['total'],
                         'voter_turnout': data[d]['voter_turnout'],
                         "ordinary_polling_stations": ordinary,
                         "advance_polling_stations": advance,
                         "special_voting_rules": special
                         }
            full_data_list.append(full_data)
    return full_data_list


def get_second_table_data(data):
    print("get_second_table_data")
    browser.back()
    elements = browser.find_elements_by_tag_name('a')
    for e in elements:
        link = e.get_attribute('href')
        # opening the table tab
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
            if 'by voting method' in l.text:
                l.click()
                full_data = get_voting_methods(data)
            # elif "votes, by voting method" in l.text:
            #     l.click()
            #     full_data = get_voting_methods(data)
            elif 'invalid_ballots' not in data[0]:
                if "ballots cast and" in l.text:
                #if "Table 3" not in l.text:
                    l.click()
                    full_data = get_ballot_count(data)

        except Exception as e:
            pass
    return full_data


def get_ballot_count(data):
    print('get_ballot_count')
    full_data_list = []
    tables = browser.find_elements_by_tag_name('table')
    for t in tables:
        if "ballots cast" in t.text:
            rows = t.find_elements_by_tag_name('tr')
            for r in rows[2:]:
                try:
                    items = r.find_elements_by_tag_name('td')
                    province = items[0].text
                    rejected = items[4].text.replace(',', '')
                    turnout = items[6].text.split('.')[0]

                    for d in range(0, len(data)):
                        if data[d]['province'] == province:
                            full_data = {'election': data[d]['election'], 'province': province,
                                                 'invalid_ballots': rejected,
                                                 'total': data[d]['total'],
                                                 'voter_turnout': turnout,
                                                 "ordinary_polling_stations": data[d]['ordinary_polling_stations'],
                                                 "advance_polling_stations": data[d]['advance_polling_stations'],
                                                 "special_voting_rules": data[d]['special_voting_rules']
                                                 }
                            full_data_list.append(full_data)
                except Exception as e:
                    items = r.find_elements_by_tag_name('td')
                    province = r.find_element_by_tag_name('th').text
                    rejected = items[3].text.replace(',', '')
                    turnout = items[5].text.split('.')[0]

                    for d in range(0, len(data)):
                        if data[d]['province'] == province:
                            full_data = {'election': data[d]['election'], 'province': province,
                                         'invalid_ballots': rejected,
                                         'total': data[d]['total'],
                                         'voter_turnout': turnout,
                                         "ordinary_polling_stations": data[d]['ordinary_polling_stations'],
                                         "advance_polling_stations": data[d]['advance_polling_stations'],
                                         "special_voting_rules": data[d]['special_voting_rules']
                                         }
                            full_data_list.append(full_data)
    return full_data_list


def get_voting_methods(data):
    print('get_voting_methods')
    districts = []
    full_data_list = []
    tables = browser.find_elements_by_tag_name('table')
    for t in tables:
        if "voting" in t.text:
            table = t
    rows = table.find_elements_by_tag_name('tr')
    try:
        for r in rows[3:-1]:
            province = r.find_element_by_tag_name('th').text
            ordinary = int(r.find_elements_by_tag_name('td')[0].text.replace(',', ''))
            ordinary += int(r.find_elements_by_tag_name('td')[1].text.replace(',', ''))
            advance = int(r.find_elements_by_tag_name('td')[2].text.replace(',', ''))
            special = int(r.find_elements_by_tag_name('td')[3].text.replace(',', ''))
            for d in range(0, 13):
                if data[d]['province'] == province:
                    full_data = {'election': data[d]['election'], 'province': province,
                                     'invalid_ballots': data[d]['invalid_ballots'],
                                     'total=': data[d]['total'],
                                     'voter_turnout': data[d]['voter_turnout'],
                                     "ordinary_polling_stations": ordinary,
                                     "advance_polling_stations": advance,
                                     "special_voting_rules": special
                                     }
                    full_data_list.append(full_data)
    except:
        district_list = rows[0].find_elements_by_tag_name('th')
        for d in district_list[1:]:
            print(d.text)
            districts.append(d.text)
            try:
                item = 0
                for d in districts:
                    ordinary = rows[2].find_elements_by_tag_name('td')[item].text.replace(',', '')
                    advance = rows[3].find_elements_by_tag_name('td')[item].text.replace(',', '')
                    special = int(rows[4].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                    special += int(rows[5].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                    total = rows[6].find_elements_by_tag_name('td')[item].text.replace(',', '')
                    item += 2
                    for i in range(0, len(districts)):
                        if data[i]['province'] == d:
                            full_data = {'election': data[i]['election'], 'province': province,
                                             'invalid_ballots': data[i]['invalid_ballots'],
                                             'total': data[i]['total'],
                                             'voter_turnout': data[i]['voter_turnout'],
                                             "ordinary_polling_stations": ordinary,
                                             "advance_polling_stations": advance,
                                             "special_voting_rules": special
                                             }
                            full_data_list.append(full_data)
            except:
                pass
    if not full_data_list:
        full_data_list = get_vote_methods_from_nested_table(data)
    return full_data_list


def get_prov_terr_id_from_district(province):
    print("get_prov_terr_id_from_district")
    province = province.replace('–', '--')
    if pd.notna(province):
        df = districts
        value = df.loc[df["district_name"] == province]['province_territory_id'].values[0]
        try:
            return int(value)
        except Exception:
            return value


def get_prov_terr_id(province):
    print('get_prov_terr_id')
    if pd.notna(province):
        df = scraper_utils.divisions
        value = df.loc[df["division"] == province]['id'].values[0]
        try:
            return int(value)
        except Exception:
            return value


def get_election_with_frame(input_value, url):
    print("get_election_with_frame")
    browser.get(url + f'{input_value}/table3.html')
    election_name = ''
    try:
        election_name = browser.find_element_by_id('EventName')
    except:
        pass
    if election_name == '':
        election_name = browser.find_element_by_id('EventNameE')
    election_name = election_name.text
    partial_data = get_province_data_from_frame_table(election_name)
    data = get_second_frame_table_data(input_value, url, partial_data)
    return data


def get_second_frame_table_data(input_value, url, data):
    full_data_list = []
    browser.get(url + f'{input_value}/table5.html')
    table = browser.find_element_by_tag_name("tbody")
    rows = table.find_elements_by_tag_name('tr')
    for r in rows[9:-3]:
        if "Nunavut" in r.text:
            break
        else:
            items = r.find_elements_by_tag_name('td')
        province = items[0].text.strip()
        try:
            province = province.split('/')[0]
        except:
            pass
        try:
            ordinary = int(items[1].text.replace(' ', ''))
            ordinary += int(items[2].text.replace(' ', ''))
            advance = int(items[3].text.replace(' ', ''))
            special = int(items[4].text.replace(' ', ''))
        except:
            pass

        for d in range(0, 12):
            if data[d]['province'] == province:
                full_data = {'election': data[d]['election'], 'province': province,
                             'invalid_ballots': data[d]['invalid_ballots'],
                             'total': data[d]['total'],
                             'voter_turnout': data[d]['voter_turnout'],
                             "ordinary_polling_stations": ordinary,
                             "advance_polling_stations": advance,
                             "special_voting_rules": special
                             }
                full_data_list.append(full_data)
    return full_data_list


def get_election():
    print("get_election")
    try:
        election_name = browser.find_element_by_id('EventName')
        if election_name is None:
            election_name = browser.find_element_by_id('EventNameE')
        election_name = election_name.text
        print(election_name)
        partial_data = get_province_data(election_name)
        data = get_second_table_data(partial_data)
        return data
    except:
        if browser.find_elements_by_tag_name('b'):
            data = browser.find_elements_by_tag_name('b')
        elif browser.find_elements_by_tag_name('h1'):
            data = browser.find_elements_by_tag_name('h1')
        if not data:
            data = get_district_table()
            return (data)
        for e in data:
            if 'election' in e.text:
                election_name = e.text
                election_name = election_name.split(':')[0]
                print(election_name)
                partial_data = get_province_data_from_alt_table(election_name)
                return get_second_alt_table(partial_data)


def get_district_table():
    print("get_district_table")
    try:
        title = browser.find_element_by_tag_name('h1').text
        title = title.split(':')[0]
        title = title.split('–')[0]
    except:
        title = browser.find_element_by_tag_name('table').text
        title = title.split(':')[0]
    print(title)
    return get_district_data(title)


def get_province_data_from_alt_table(election_name):
    print("get_province_data_from_alt_table")
    provincial_data_list = []
    table = browser.find_elements_by_tag_name("tbody")[1]
    rows = table.find_elements_by_tag_name('tr')
    for r in rows[1:]:
        print(r.text)
        try:
            if 'Totals' not in r.text:
                items = r.find_elements_by_tag_name('td')
                province = items[0].text.strip()
                province = province.split('/')[0]
                invalid_ballots = items[5].text.replace(' ', '')
                total = items[7].text.replace(' ', '')
                invalid_ballots = invalid_ballots.replace(',', '')
                total = total.replace(',', '')
                voter_turnout = items[8].text.split('.')[0]
                provincial_data = {'election': election_name, 'province': province,
                                   'invalid_ballots': invalid_ballots,
                                   'total': total,
                                   'voter_turnout': voter_turnout
                                   }
                provincial_data_list.append(provincial_data)
        except:
            if 'Totals' not in r.text:
                items = r.find_elements_by_tag_name('td')
                province = items[0].text.strip()
                province = province.split('/')[0]
                invalid_ballots = items[4].text.replace(' ', '')
                total = items[5].text.replace(' ', '')
                invalid_ballots = invalid_ballots.replace(',', '')
                total = total.replace(',', '')
                voter_turnout = items[6].text.split('.')[0]
                provincial_data = {'election': election_name, 'province': province,
                                   'invalid_ballots': invalid_ballots,
                                   'total': total,
                                   'voter_turnout': voter_turnout
                                   }
                provincial_data_list.append(provincial_data)
    return provincial_data_list


def get_province_data_from_frame_table(election_name):
    print("get_province_data_from_frame_table")
    provincial_data_list = []
    table = browser.find_element_by_tag_name("tbody")
    rows = table.find_elements_by_tag_name('tr')
    for r in rows[8:-3]:
        if "Nunavut" in r.text:
            break
        else:
            items = r.find_elements_by_tag_name('td')
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
            total = items[7].text.replace(' ', '')
            total = total.replace(',', '')
            voter_turnout = items[8].text.split('.')[0]
        except:
            pass
        if province != '':
            provincial_data = {'election': election_name, 'province': province,
                               'valid_ballots': valid_ballots,
                               'invalid_ballots': invalid_ballots,
                               'total': total,
                               'voter_turnout': voter_turnout
                               }
            provincial_data_list.append(provincial_data)
    # print(provincial_data_list)
    return provincial_data_list


def get_district_data(election):
    print("get_district_data")
    provincial_data_list = []
    districts = []
    tables = browser.find_elements_by_tag_name("table")
    for t in tables:

        if "Table 4" in t.text:
            print("called this one")
            table = t
            rows = table.find_elements_by_tag_name('tr')
            for r in rows[1:]:
                print(r.text)
                try:
                    district = r.find_element_by_tag_name('th').text
                    invalid_ballots = r.find_elements_by_tag_name('td')[3].text.replace(',', '')
                    total = r.find_elements_by_tag_name('td')[4].text.replace(',', '')
                    voter_turnout = r.find_elements_by_tag_name('td')[5].text.split('.'[0])
                except:
                    district = r.find_elements_by_tag_name('td')[0].text
                    invalid_ballots = r.find_elements_by_tag_name('td')[4].text.replace(',', '')
                    total = r.find_elements_by_tag_name('td')[5].text.replace(',', '')
                    voter_turnout = r.find_elements_by_tag_name('td')[6].text.split('.')[0]
                voting_detail = {
                    "province": district,
                    "election": election,
                    "invalid_ballots": invalid_ballots,
                    "total": total,
                    "voter_turnout": voter_turnout
                    }
                provincial_data_list.append(voting_detail)
        if "voting method" in t.text.lower():
            print('called the second one')
            table = t
            rows = table.find_elements_by_tag_name('tr')
            district_list = rows[0].find_elements_by_tag_name('th')
            for d in district_list[1:]:
                districts.append(d.text)
            for r in rows:
                try:
                    item = 1
                    for d in districts:
                        ordinary_polling_stations = rows[2].find_elements_by_tag_name('td')[item].text.replace(',', '')
                        advance_polling_stations = rows[3].find_elements_by_tag_name('td')[item].text.replace(',', '')
                        special_voting_rules = int(rows[4].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                        special_voting_rules += int(rows[5].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                        total = rows[6].find_elements_by_tag_name('td')[item].text.replace(',', '')
                        item += 2
                        voting_detail = {"province": d,
                                         "election": election,
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
                        total = rows[6].find_elements_by_tag_name('td')[item].text.replace(',', '')
                        item += 2
                        voting_detail = {"province": d,
                                         "election": election,
                                         "ordinary_polling_stations": ordinary_polling_stations,
                                         "advance_polling_stations": advance_polling_stations,
                                         "special_voting_rules": special_voting_rules,
                                         "total": total}
                        provincial_data_list.append(voting_detail)
    if provincial_data_list:
        #print(provincial_data_list)
        full_data_list = get_second_table_data(provincial_data_list)
    if not provincial_data_list:
        #print(provincial_data_list)
        provincial_data_list = get_data_from_nested_tables(election)
        full_data_list = get_second_table_data(provincial_data_list)
    return (full_data_list)


def get_data_from_nested_tables(election):
    print('get_data_from_nested_tables')
    provincial_data_list = []
    outside_table = browser.find_element_by_tag_name('table')
    table = outside_table.find_element_by_tag_name('table')
    data_row = table.find_elements_by_tag_name('tr')[2]
    # print(data_row.text)
    columns = data_row.find_elements_by_tag_name('td')
    provinces = columns[0].text.split('\n')
    invalid_ballots = columns[5].text.split('\n')
    total = columns[7].text.split('\n')
    voter_turnout = columns[8].text.split('\n')
    for i in range(0, 12):
        provincial_data = {'election': election, 'province': provinces[i],
                           'invalid_ballots': invalid_ballots[i].replace(' ', ''),
                           'total': total[i].replace(' ', ''),
                           'voter_turnout': voter_turnout[i].split('.')[0]
                           }
        provincial_data_list.append(provincial_data)

    return provincial_data_list


def get_vote_methods_from_nested_table(data):
    print('get_vote_methods_from_nested_table')
    full_data_list = []
    outside_table = browser.find_element_by_tag_name('table')
    table = outside_table.find_element_by_tag_name('table')
    data_row = table.find_elements_by_tag_name('tr')[2]
    columns = data_row.find_elements_by_tag_name('td')
    provinces = columns[0].text.split('\n')
    ordinary_stationary_list = columns[1].text.split('\n')
    ordinary_mobile_list = columns[2].text.split('\n')
    advance_list = columns[3].text.split('\n')
    special_list = columns[4].text.split('\n')
    try:
        for i in range(0, 12):
            province = provinces[i]
            stationary = ordinary_stationary_list[i].replace(' ', '')
            mobile = ordinary_mobile_list[i].replace(' ', '')
            ordinary = int(stationary.replace('*', '')) + int(mobile.replace('*', ''))
            advance = advance_list[i].replace(' ', '')
            special = special_list[i].replace(' ', '')
            for d in range(0, 12):
                if data[d]['province'] == province:
                    full_data = {'election': data[d]['election'], 'province': province,
                                 'invalid_ballots': data[d]['invalid_ballots'],
                                 'total': data[d]['total'],
                                 'voter_turnout': data[d]['voter_turnout'],
                                 "ordinary_polling_stations": ordinary,
                                 "advance_polling_stations": advance,
                                 "special_voting_rules": special
                                 }
                    full_data_list.append(full_data)
    except Exception as e:
        print(e)
    return full_data_list


def get_paragraph_table(election):
    print("get_paragraph_table")
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
    # print(provincial_data_list)
    return provincial_data_list


def get_province_data(election):
    print("get_province_data")
    provincial_data_list = []
    table = browser.find_element_by_tag_name("tbody")
    rows = table.find_elements_by_tag_name('tr')
    for r in rows:
        try:
            province = r.find_element_by_tag_name('th').text
            if "Totals" not in province:
                items = r.find_elements_by_tag_name('td')
                provincial_data = {'election': election, 'province': province,
                                   'invalid_ballots': items[4].text.replace(',', ''),
                                   'total': items[6].text.replace(',', ''),
                                   'voter_turnout': items[7].text.split('.')[0]
                                   }
                provincial_data_list.append(provincial_data)
        except Exception as e:
            print(e)
    #print(provincial_data_list)
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
    data.extend(get_table_data(url) for url in urls)

    #get_table_data('https://www.elections.ca//res/rep/off/ovr2019app/home.html')
    # lambda_obj = lambda x: (x is not None)
    #
    # list_out = list(filter(lambda_obj, data))
    #
    # flat_ls = [item for sublist in list_out for item in sublist]
    # print(data)
    # with Pool(processes=4) as pool:
    #     data = pool.map(scrape, urls)
    # print(data_list)
    # row_data = [get_row_data(d) for d in flat_ls]
    # scraper_utils.write_data(row_data)

    print('Complete!')
