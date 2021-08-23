import sys
import os
from pathlib import Path
import time
from selenium import webdriver
import scraper_utils
from scraper_utils import ElectionVotesScraperUtils
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
TABLE = 'ca_election_votes'
MAIN_URL = 'https://www.elections.ca/'
ELECTIONS_URL = MAIN_URL + 'content.aspx?section=ele&dir=pas&document=index&lang=e'
THREADS_FOR_POOL = 12

scraper_utils = ElectionVotesScraperUtils(COUNTRY, TABLE)
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
    if not data:
        data = []
    if data:
        row_list = []
        for i in data:
            province = i['province']
            if province == 'Newfoundland':
                province = 'Newfoundland and Labrador'
            if province == "Labrador":
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
                    row_info = {'election_id': election_id, 'province_territory_id': province_id,
                                 'invalid_ballots': i['invalid_ballots'],
                                 'total': i['total'],
                                 'voter_turnout': i['voter_turnout'],
                                 "ordinary_stationary": i["ordinary_stationary"],
                                 "ordinary_mobile": i["ordinary_mobile"],
                                 "advance_polling": i["advance_polling"],
                                 "special_voting_rules": i["special_voting_rules"]
                                 }
                    row_list.append(row_info)
        if row_list is None:
            row_list = []
        #print(row_list)
        #print()
        return row_list


def get_second_alt_table(data):
    #print("get_second_alt_table_data")
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
    #print('get_voting_methods_alt_table')
    full_data_list = []
    table = browser.find_elements_by_tag_name("tbody")[1]
    rows = table.find_elements_by_tag_name('tr')
    try:
        for r in rows[2:]:
            if 'Totals' not in r.text:
                items = r.find_elements_by_tag_name('td')
                province = items[0].text.strip()
                province = province.split('/')[0]
                ordinary = int(items[1].text.replace(' ', ''))
                mobile = int(items[2].text.replace(' ', ''))
                advance = items[3].text.replace(' ', '')
                special = items[4].text.split('.')[0]

        for d in range(0, 13):
            if data[d]['province'] == province:
                full_data = {'election': data[d]['election'], 'province': province,
                             'invalid_ballots': data[d]['invalid_ballots'],
                             'total': data[d]['total'],
                             'voter_turnout': data[d]['voter_turnout'],
                             "ordinary_stationary": ordinary,
                             "ordinary_mobile": mobile,
                             "advance_polling": advance,
                             "special_voting_rules": special
                             }
                full_data_list.append(full_data)
    except:
        return get_voting_methods(data)
    return full_data_list


def get_second_table_data(data):
    #print("get_second_table_data")
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
            if 'invalid_ballots' not in data[0]:
                if "ballots cast and" in l.text:
                    # if "Table 3" not in l.text:
                    l.click()
                    full_data = get_ballot_count(data)
            elif 'by voting method' in l.text:
                l.click()
                full_data = get_voting_methods(data)

        except Exception as e:
            pass
    return full_data


def get_election_id(data):
    election = data['election']
    province = data['province']
    province = province.replace('–', '-')
    province = province.replace(' ', '_')
    if 'by-election' in election.lower():
        try:
            date = dparser.parse(election, fuzzy=True)
            date_name = date.strftime("%Y_%m_%d")
            election_name = 'by_election_' + date_name
        except Exception as e:
            print(e)
        if pd.notna(province):
            df = elections
            value = df.loc[df['election_name'].str.contains(election_name)]['id'].values[0]
            try:
                return int(value)
            except Exception as e:
                return value
    if 'general' in election.lower():
        general_elections = {
            'fiftieth': '50',
            'forty-ninth': '49',
            'forty-eighth': '48',
            'forty-seventh': '47',
            'forty-sixth': '46',
            'forty-fifth': '45',
            'forty-fourth': '44',
            'forty-third': '43',
            'forty-second': '42',
            'forty-first': '41',
            'fortieth': '40',
            'thirty-ninth': '39',
            'thirty-eighth': '38',
            'thirty-seventh': '37',
            'thirty-sixth': '36'
        }
        election = election.split(' ')[0].lower()
        e_number = general_elections.get(election)
        election_name = e_number + '_general_election'
        if pd.notna(province):
            df = elections
            value = df.loc[df['election_name'].str.contains(election_name)]['id'].values[0]
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


def get_ballot_count(data):
    #print('get_ballot_count')
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
                                                 "ordinary_stationary": data[d]['ordinary_stationary'],
                                                 'ordinary_mobile': data[d]['ordinary_mobile'],
                                                 "advance_polling": data[d]['advance_polling'],
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
                                         'ordinary_stationary': data[d]['ordinary_stationary'],
                                         'ordinary_mobile': data[d]['ordinary_mobile'],
                                         "advance_polling": data[d]['advance_polling'],
                                         "special_voting_rules": data[d]['special_voting_rules']
                                         }
                            full_data_list.append(full_data)
    return full_data_list


def get_voting_methods(data):
    #print('get_voting_methods')
    districts = []
    full_data_list = []
    tables = browser.find_elements_by_tag_name('table')
    for t in tables:
        if "voting" in t.text.lower():
            table = t
    rows = table.find_elements_by_tag_name('tr')
    try:
        for r in rows[3:-1]:
            province = r.find_element_by_tag_name('th').text.replace('\n', ' ')
            ordinary = int(r.find_elements_by_tag_name('td')[0].text.replace(',', ''))
            mobile = int(r.find_elements_by_tag_name('td')[1].text.replace(',', ''))
            advance = int(r.find_elements_by_tag_name('td')[2].text.replace(',', ''))
            special = int(r.find_elements_by_tag_name('td')[3].text.replace(',', ''))
            for d in range(0, 13):
                if data[d]['province'] == province:
                    full_data = {'election': data[d]['election'], 'province': province,
                                     'invalid_ballots': data[d]['invalid_ballots'],
                                     'total': data[d]['total'],
                                     'voter_turnout': data[d]['voter_turnout'],
                                     "ordinary_stationary": ordinary,
                                     "ordinary_mobile": mobile,
                                     "advance_polling": advance,
                                     "special_voting_rules": special
                                     }
                    full_data_list.append(full_data)
    except:
        pass
    try:
        district_list = rows[0].find_elements_by_tag_name('th')
        if not district_list:
            district_list = rows[1].find_elements_by_tag_name('td')
        for d in district_list:
            districts.append(d.text)
        try:
            item = 0
            for d in districts:
                ordinary = rows[3].find_elements_by_tag_name('td')[item].text.replace(',', '')
                advance = rows[4].find_elements_by_tag_name('td')[item].text.replace(',', '')
                special = int(rows[5].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                special += int(rows[6].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                item += 2
                for i in range(0, len(districts)):
                    if data[i]['province'] == d:
                        full_data = {'election': data[i]['election'], 'province': province,
                                             'invalid_ballots': data[i]['invalid_ballots'],
                                             'total': data[i]['total'],
                                             'voter_turnout': data[i]['voter_turnout'],
                                             "ordinary_stationary": ordinary,
                                             "ordinary_mobile": 0,
                                             "advance_polling": advance,
                                             "special_voting_rules": special
                                             }
                        full_data_list.append(full_data)
        except:
            item = 1
            for d in districts:
                ordinary = rows[2].find_elements_by_tag_name('td')[item].text.replace(',', '')
                advance = rows[3].find_elements_by_tag_name('td')[item].text.replace(',', '')
                special = int(rows[4].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                special += int(rows[5].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                item += 2
                for i in range(0, len(districts)):
                    if data[i]['province'] == d:
                        full_data = {'election': data[i]['election'], 'province': d,
                                     'invalid_ballots': data[i]['invalid_ballots'],
                                     'total': data[i]['total'],
                                     'voter_turnout': data[i]['voter_turnout'],
                                     "ordinary_stationary": ordinary,
                                     "ordinary_mobile": 0,
                                     "advance_polling": advance,
                                     "special_voting_rules": special
                                     }
                        full_data_list.append(full_data)
    except:
        pass
    if not full_data_list:
        try:
            item = 1
            for d in districts:
                ordinary = rows[3].find_elements_by_tag_name('td')[item].text.replace(',', '')
                ordinary = ordinary.replace(' ', '')
                advance = rows[4].find_elements_by_tag_name('td')[item].text.replace(',', '')
                advance = advance.replace(' ', '')
                special = int(rows[5].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                special += int(rows[6].find_elements_by_tag_name('td')[item].text.replace(',', ''))
                item += 2
                for i in range(0, len(districts)):
                    if data[i]['province'] == d:
                        full_data = {'election': data[i]['election'], 'province': d,
                                         'invalid_ballots': data[i]['invalid_ballots'],
                                         'total': data[i]['total'],
                                         'voter_turnout': data[i]['voter_turnout'],
                                         "ordinary_stationary": ordinary,
                                         "ordinary_mobile": 0,
                                         "advance_polling": advance,
                                         "special_voting_rules": special
                                         }
                        full_data_list.append(full_data)
        except:
            pass
    if not full_data_list:
        full_data_list = get_vote_methods_from_nested_table(data)
    return full_data_list


def get_election_with_frame(input_value, url):
    #print("get_election_with_frame")
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
            mobile = int(items[2].text.replace(' ', ''))
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
                             "ordinary_stationary": ordinary,
                             "ordinary_mobile": mobile,
                             "advance_polling": advance,
                             "special_voting_rules": special
                             }
                full_data_list.append(full_data)
    return full_data_list


def get_election():
    #print("get_election")
    try:
        election_name = browser.find_element_by_id('EventName')
        if election_name is None:
            election_name = browser.find_element_by_id('EventNameE')
        election_name = election_name.text
        #print(election_name)
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
                #print(election_name)
                partial_data = get_province_data_from_alt_table(election_name)
                return get_second_alt_table(partial_data)


def get_district_table():
    #print("get_district_table")
    try:
        title = browser.find_element_by_tag_name('h1').text
        title = title.split(':')[0]
        title = title.split('–')[0]
    except:
        title = browser.find_element_by_tag_name('table').text
        title = title.split(':')[0]
    #print(title)
    return get_district_data(title)


def get_province_data_from_alt_table(election_name):
    #print("get_province_data_from_alt_table")
    provincial_data_list = []
    table = browser.find_elements_by_tag_name("tbody")[1]
    rows = table.find_elements_by_tag_name('tr')
    for r in rows[1:]:
        try:
            if 'Totals' not in r.text:
                items = r.find_elements_by_tag_name('td')
                province = items[0].text.strip()
                if "No." in province:
                    continue
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
    #print("get_province_data_from_frame_table")
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
    return provincial_data_list


def get_district_data(election):
    #print("get_district_data")
    provincial_data_list = []
    districts = []
    tables = browser.find_elements_by_tag_name("table")
    for t in tables:
        if "Table 4" in t.text:
            print("called this one")
            table = t
            rows = table.find_elements_by_tag_name('tr')
            for r in rows[1:]:
                try:
                    try:
                        district = r.find_element_by_tag_name('th').text
                        invalid_ballots = r.find_elements_by_tag_name('td')[3].text.replace(',', '')
                        total = r.find_elements_by_tag_name('td')[4].text.replace(',', '')
                        voter_turnout = r.find_elements_by_tag_name('td')[5].text.split('.'[0])
                        invalid_ballots = invalid_ballots.replace(' ', '')
                        total = total.replace(' ', '')
                    except:
                        pass
                    try:
                        district = r.find_elements_by_tag_name('td')[0].text
                        invalid_ballots = r.find_elements_by_tag_name('td')[4].text.replace(',', '')
                        total = r.find_elements_by_tag_name('td')[5].text.replace(',', '')
                        voter_turnout = r.find_elements_by_tag_name('td')[6].text.split('.')[0]
                        invalid_ballots = invalid_ballots.replace(' ', '')
                        total = total.replace(' ', '')
                    except:
                        pass
                    voting_detail = {
                        "province": district,
                        "election": election,
                        "invalid_ballots": invalid_ballots,
                        "total": total,
                        "voter_turnout": voter_turnout
                        }
                    provincial_data_list.append(voting_detail)
                except:
                    pass
        if "Valid ballots" in t.text:
            #print("called this one")
            table = t
            rows = table.find_elements_by_tag_name('tr')
            for r in rows[1:]:
                try:
                    district = r.find_element_by_tag_name('th').text
                    invalid_ballots = r.find_elements_by_tag_name('td')[3].text.replace(',', '')
                    total = r.find_elements_by_tag_name('td')[4].text.replace(',', '')
                    voter_turnout = r.find_elements_by_tag_name('td')[5].text.split('.'[0])
                    invalid_ballots = invalid_ballots.replace(' ', '')
                    total = total.replace(' ', '')
                except:
                    try:
                        district = r.find_elements_by_tag_name('td')[0].text
                        invalid_ballots = r.find_elements_by_tag_name('td')[4].text.replace(',', '')
                        total = r.find_elements_by_tag_name('td')[5].text.replace(',', '')
                        invalid_ballots = invalid_ballots.replace(' ', '')
                        total = total.replace(' ', '')
                    except:
                        pass
                    try:
                        voter_turnout = r.find_elements_by_tag_name('td')[6].text.split('.')[0]
                    except:
                        voter_turnout = 0
                voting_detail = {
                    "province": district,
                    "election": election,
                    "invalid_ballots": invalid_ballots,
                    "total": total,
                    "voter_turnout": voter_turnout
                }
                provincial_data_list.append(voting_detail)
        if "voting method" in t.text.lower():
            #print('called the second one')
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
                                         "ordinary_stationary": ordinary_polling_stations,
                                         "ordinary_mobile": 0,
                                         "advance_polling": advance_polling_stations,
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
                                         "ordinary_stationary": ordinary_polling_stations,
                                         "ordinary_mobile": 0,
                                         "advance_polling": advance_polling_stations,
                                         "special_voting_rules": special_voting_rules,
                                         "total": total}
                        provincial_data_list.append(voting_detail)

    if provincial_data_list:
        full_data_list = get_second_table_data(provincial_data_list)
    if not provincial_data_list:
        provincial_data_list = get_data_from_nested_tables(election)
        if provincial_data_list:
            full_data_list = get_second_table_data(provincial_data_list)
        else:
            tables = browser.find_elements_by_tag_name('table')
            for t in tables:
                if "TABLE 4" in t.text:
                    table = t.find_element_by_tag_name('table')
                    rows = table.find_elements_by_tag_name('tr')
                    for r in rows[2:]:
                        #print(r.text)
                       #print("NEW ROW")
                        items = r.find_elements_by_tag_name('td')
                        province = items[0].text
                        invalid_ballots = items[4].text.replace(' ', '')
                        total = items[5].text.replace(' ', '')
                        voter_turnout = items[6].text.split(".")[0]
                        full_data_list = {
                            "province": province,
                            "election": election,
                            "invalid_ballots": invalid_ballots,
                            "total": total,
                            "voter_turnout": voter_turnout,
                            "ordinary_stationary": 0,
                            "ordinary_mobile": 0,
                            "advance_polling": 0,
                            "special_voting_rules": 0
                            }
    return (full_data_list)


def get_data_from_nested_tables(election):
    #print('get_data_from_nested_tables')
    provincial_data_list = []
    try:
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
    except:
        pass
    #print(provincial_data_list)
    return provincial_data_list


def get_vote_methods_from_nested_table(data):
    #print('get_vote_methods_from_nested_table')
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
            ordinary = int(stationary.replace('*', ''))
            advance = advance_list[i].replace(' ', '')
            special = special_list[i].replace(' ', '')
            for d in range(0, 12):
                if data[d]['province'] == province:
                    full_data = {'election': data[d]['election'], 'province': province,
                                 'invalid_ballots': data[d]['invalid_ballots'],
                                 'total': data[d]['total'],
                                 'voter_turnout': data[d]['voter_turnout'],
                                 "ordinary_stationary": ordinary,
                                 "ordinary_mobile": mobile,
                                 "advance_polling": advance,
                                 "special_voting_rules": special
                                 }
                    full_data_list.append(full_data)
    except Exception as e:
        print(e)
    return full_data_list


def get_province_data(election):
    #print("get_province_data")
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
    try:
        row = scraper_utils.initialize_row()
        row.province_territory_id = int(data['province_territory_id'])
        row.election_id = int(data['election_id'])
        row.ordinary_stationary = int(data['ordinary_stationary'])
        row.ordinary_mobile = int(data['ordinary_mobile'])
        row.advanced_polling = int(data['advance_polling'])
        row.special_voting_rules = int(data['special_voting_rules'])
        row.invalid_votes = int(data['invalid_ballots'])
        row.voter_turnout = int(data['voter_turnout'])
        row.total = int(data['total'])
    except Exception as e:
        print(e)
    return row


if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')
    urls = get_urls()
    data = []
    data_list = []
    data.extend(get_table_data(url) for url in urls)

    #get_table_data('https://www.elections.ca/content.aspx?section=res&document=index&dir=rep/off/dec3097&lang=e')
    lambda_obj = lambda x: (x is not None)

    list_out = list(filter(lambda_obj, data))

    flat_ls = [item for sublist in list_out for item in sublist]
    #print(data)
    # with Pool(processes=4) as pool:
    #     data = pool.map(scrape, urls)
    # print(data_list)
    row_data = [get_row_data(d) for d in flat_ls]
    scraper_utils.write_data(row_data)

    print('Complete!')
