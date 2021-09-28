import sys
import os
from datetime import datetime
from pathlib import Path
import time
from selenium import webdriver
from scraper_utils import OutflowScraperUtils
from database import CursorFromConnectionFromPool
import pandas as pd
import dateutil.parser as dparser

NODES_TO_ROOT = 4
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

PATH = "../../../web_drivers/chrome_win_93.0.4577.15/chromedriver.exe"
browser = webdriver.Chrome(PATH)

# https://www.elections.ca/WPAPPS/WPF/EN/Home/Index
COUNTRY = 'ca'
TABLE = 'ca_outflows'
MAIN_URL = 'https://www.elections.ca'
ELECTION_FINANCES_URL = MAIN_URL + '/WPAPPS/WPF/EN/Home/Index'

#
scraper_utils = OutflowScraperUtils(COUNTRY, TABLE)
crawl_delay = scraper_utils.get_crawl_delay(MAIN_URL)

with CursorFromConnectionFromPool() as cur:
    try:
        query = 'SELECT * FROM ca_candidate_election_details'
        cur.execute(query)
        candidate_election_details = cur.fetchall()

        query = 'SELECT * FROM ca_candidates'
        cur.execute(query)
        candidates = cur.fetchall()

        query = 'SELECT * FROM ca_elections'
        cur.execute(query)
        elections_table = cur.fetchall()

        query = 'SELECT * FROM ca_candidate_election_finances'
        cur.execute(query)
        finances_table = cur.fetchall()

    except Exception as e:
        sys.exit(
            f'An exception occurred retrieving tables from database:\n{e}')

    candidates_election = pd.DataFrame(candidate_election_details)
    candidates_table = pd.DataFrame(candidates)
    elections = pd.DataFrame(elections_table)
    finances_id = pd.DataFrame(finances_table)


def get_data():
    data = []
    options1 = get_first_list_of_options()
    for option in options1:
        second_options = get_second_list_of_options(option, options1)
        for o_2 in second_options:
            data.extend(get_candidate_pages(option, o_2))
    return data


def get_first_list_of_options():
    option_list = []
    browser.get(ELECTION_FINANCES_URL)
    select = browser.find_element_by_tag_name('select')
    select.click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for o_1 in options:
        if "Select" not in o_1.text:
            option_list.append(o_1.text)
    option_list = [x for x in option_list if x]
    return option_list


def get_second_list_of_options(option, option_list):
    option_2_list = []
    browser.get(ELECTION_FINANCES_URL)
    select = browser.find_element_by_tag_name('select')
    select.click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for o in options:
        if option in o.text:
            o.click()
    time.sleep(1)
    select_2 = browser.find_elements_by_tag_name('select')
    select_2[1].click()
    time.sleep(1)
    options_2 = browser.find_elements_by_tag_name('option')
    for o_2 in options_2:
        if "Select" not in o_2.text:
            option_2_list.append(o_2.text)
    option_2_list = [x for x in option_2_list if x not in option_list]
    option_2_list = [x for x in option_2_list if x]
    return option_2_list


def get_candidate_pages(option, o_2):
    candidate_election_finances_list = []
    browser.get(ELECTION_FINANCES_URL)
    select = browser.find_element_by_tag_name('select')
    select.click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for o in options:
        if option in o.text:
            o.click()
    time.sleep(1)
    select_2 = browser.find_elements_by_tag_name('select')
    select_2[1].click()
    time.sleep(1)
    options_2 = browser.find_elements_by_tag_name('option')
    for o in options_2:
        if o_2 in o.text:
            o.click()
            time.sleep(2)
    select_3 = browser.find_element_by_id('reportTypeList')
    select_3.click()
    options_3 = browser.find_elements_by_tag_name('option')
    for o_3 in options_3:
        if "Campaign Returns" in o_3.text:
            o_3.click()
            time.sleep(2)
    search_button = browser.find_element_by_id('SearchButton')
    search_button.click()
    current_url = browser.current_url
    candidate_list = search_candidates(current_url)
    candidate_election_finances_list.extend(get_candidate_election_details(candidate_list))

    return candidate_election_finances_list


def search_candidates(url):
    candidate_list = []
    browser.get(url)
    browser.find_element_by_id('button3').click()
    time.sleep(3)
    #browser.find_element_by_xpath('//*[@id="SelectedClientIds"]/option[1]').click()
    browser.find_element_by_id('SelectAllCandidates').click()
    time.sleep(3)
    browser.find_element_by_id('SearchSelected').click()

    while True:
        election = browser.find_element_by_id('eventname').text
        candidate = browser.find_element_by_id('ename1').text
        party_district = browser.find_element_by_id('partydistrict1').text
        browser.find_element_by_id('SelectedPart').click()
        time.sleep(1)
        options = browser.find_elements_by_tag_name('option')
        for option in options:
            if 'Part  4 - Campaign Financial Summary' in option.text:
                option.click()
                break
            elif 'Part 6' in option.text:
                option.click()
                break
        browser.find_element_by_id('ReportOptions').click()
        try:
            date = browser.find_element_by_class_name('date').text
            dt_object = datetime.strptime(date, '%b %d, %Y')
            date_of_return = dt_object.strftime("%Y-%m-%d")
        except:
            date_of_return = "1212-12-12"
        unpaid_claims_detail = get_unpaid_claims_detail()
        print('unpaid claims')
        print(unpaid_claims_detail)
        total_expenses_subject_to_limit_detail = get_total_expenses_subject_to_limit_detail()
        print('total expenses')
        print(total_expenses_subject_to_limit_detail)
        personal_expenses_detail = get_personal_expenses_detail()
        print('personal expenses')
        print(personal_expenses_detail)
        other_detail = get_other_detail()

        non_monetary_transfers_sent_to_political_entities = get_non_monetary_transfers_sent_to_political_entities()
        print("other")
        print(other_detail)

        candidate_info = {'election': election, 'name': candidate, 'date_of_return': date_of_return,
                          'party_district': party_district, 'unpaid_claims_detail': unpaid_claims_detail,
                          'total_expenses_subject_to_limit_detail': total_expenses_subject_to_limit_detail,
                          'personal_expenses_detail': personal_expenses_detail,
                          'non_monetary_transfers_sent_to_political_entities': non_monetary_transfers_sent_to_political_entities,
                          'other_detail': other_detail}
        outflow_data = get_outflow_data()
        #print(candidate_info)
        candidate_info.update(outflow_data)
        print(candidate_info)
        candidate_list.append(candidate_info)

        try:
            next_candidate = browser.find_element_by_id('nextpagelink_top')
            next_candidate.click()
        except Exception as e:
            break
        time.sleep(1)

    return candidate_list


def get_candidate_election_finances_id(candidate):
    if pd.notna(candidate):
        df = finances_id
        try:
            election_finances_id = df.loc[(df['candidate_election_id'] == candidate['candidate_election_id']) &
                                          (df['date_of_return'] == candidate['date_of_return'])]['id'].values[0]
        except:
            election_finances_id = 0
    try:
        return int(election_finances_id)
    except Exception:
        return 0


def get_outflow_data():
    browser.find_element_by_id('SelectedPart').click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for option in options:
        if 'Part 3c - Summary of Electoral' in option.text:
            option.click()
            break
        elif 'Part 4 - Campaign Financial Summary' in option.text:
            option.click()
            break
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    items = browser.find_elements_by_tag_name('tr')
    for i in items:
        try:
            line_header = i.find_element_by_tag_name('th')
        except:
            pass
        try:
            if 'Election expenses limit' in line_header.text:
                expenses_limit = i.find_element_by_tag_name('td').text.replace(',', '')
            if 'Election expenses subject to the limit - Total' in line_header.text:
                total_expenses_subject_to_limit = i.find_element_by_tag_name('td').text.replace(',', '')
            if 'Personal expenses - Total' in line_header.text:
                personal_expenses = i.find_element_by_tag_name('td').text.replace(',', '')
            if 'Other expenses and outflows - Total' in line_header.text:
                other_expenses = i.find_element_by_tag_name('td').text.replace(',', '')
            if 'Total - Unpaid claim' in line_header.text:
                unpaid_claims = i.find_element_by_tag_name('td').text.replace(',', '')
            if 'Total - Paid' in line_header.text:
                campaign_expenses = i.find_element_by_tag_name('td').text.replace(',', '')
            if 'Total - Contributed or transferred' in line_header.text:
                contributed_transferred_property_or_service = i.find_element_by_tag_name('td').text.replace(',', '')
            if 'Grand total' in line_header.text:
                total_outflows = i.find_element_by_tag_name('td').text.replace(',', '')

        except:
            try:
                if 'Election expenses limit' in line_header.text:
                    expenses_limit = i.find_element_by_tag_name('td').text.replace(',', '')
                if 'TOTAL ELECTION EXPENSES SUBJECT TO THE LIMIT' in line_header.text:
                    total_expenses_subject_to_limit = i.find_element_by_tag_name('td').text.replace(',', '')
                if 'personal expenses' in line_header.text:
                    personal_expenses = i.find_element_by_tag_name('td').text.replace(',', '')
                if 'Other expenses and outflows' in line_header.text:
                    other_expenses = i.find_element_by_tag_name('td').text.replace(',', '')
                if 'Unpaid claims' in line_header.text:
                    unpaid_claims = i.find_element_by_tag_name('td').text.replace(',', '')
                if 'TOTAL ELECTORAL CAMPAIGN EXPENSES' in line_header.text:
                    campaign_expenses = i.find_element_by_tag_name('td').text.replace(',', '')
                if 'Contributed or transferred' in line_header.text:
                    contributed_transferred_property_or_service = i.find_element_by_tag_name('td').text.replace(',', '')
                if 'TOTAL CAMPAIGN CASH OUTFLOWS' in line_header.text:
                    total_outflows = i.find_element_by_tag_name('td').text.replace(',', '')

            except:
                pass

    try:
        outflow_data = {
            'expenses_limit': float(expenses_limit),
            'total_expenses_subject_to_limit': float(total_expenses_subject_to_limit),
            'personal_expenses': float(personal_expenses),
            'other_expenses': float(other_expenses),
            'campaign_expenses': float(campaign_expenses),
            'contributed_transferred_property_or_service': float(contributed_transferred_property_or_service),
            'unpaid_claims': float(unpaid_claims),
            'total_outflows': float(total_outflows)
              }
    except:
        outflow_data = {
            'expenses_limit': 0.00,
            'total_expenses_subject_to_limit': 0.00,
            'personal_expenses': 0.00,
            'other_expenses': 0.00,
            'campaign_expenses': 0.00,
            'contributed_transferred_property_or_service': 0.00,
            'unpaid_claims': 0.00,
            'total_outflows': 0.00
        }
    return outflow_data


def get_total_expenses_subject_to_limit_detail():
    details = []
    browser.find_element_by_id('SelectedPart').click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for option in options:
        if 'Part 3b - Statement of Election' in option.text:
            option.click()
            break
        # elif 'Part 3a - Statement of Electoral' in option.text:
        #     option.click()
        #     break
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    try:
        rows = browser.find_elements_by_tag_name('tr')
        for r in rows:
            try:
                items = r.find_elements_by_tag_name('td')
                date = items[1].text
                dt_object = datetime.strptime(date, '%b %d, %Y')
                date = dt_object.strftime("%Y-%m-%d")
                row_item = {'date': date,
                         'supplier': items[2].text,
                         'description': items[3].text,
                         'Cheque no.': items[4].text,
                         'Amount paid': float(items[5].text.replace(',', '')),
                         'Contributed or transferred property or service': float(
                             items[6].text.replace(',', '')),
                         'Unpaid claim': float(items[7].text.replace(',', '')),
                         'Advertising': float(items[8].text.replace(',', '')),
                         'Voter contact calling services': float(items[9].text.replace(',', '')),
                         'Office': float(items[10].text.replace(',', '')),
                         'Salaries and wages': float(items[11].text.replace(',', '')),
                         'Other': float(items[12].text.replace(',', ''))
                         }
                details.append(row_item)
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)

    return details


def get_personal_expenses_detail():
    details = []
    browser.find_element_by_id('SelectedPart').click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for option in options:
        if 'Part 3c - Statement of' in option.text:
            option.click()
            break
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    try:
        rows = browser.find_elements_by_tag_name('tr')
        for r in rows:
            try:
                items = r.find_elements_by_tag_name('td')
                date = items[1].text
                dt_object = datetime.strptime(date, '%b %d, %Y')
                date = dt_object.strftime("%Y-%m-%d")
                row_item = {'date': date,
                            'supplier': items[2].text,
                            'description': items[3].text,
                            'Cheque no.': items[4].text,
                            'Amount paid': items[5].text.replace(',', ''),
                            'Contributed or transferred property or service':
                                items[6].text.replace(',', ''),
                            'Unpaid claim': items[7].text.replace(',', ''),
                            'Transportation': items[8].text.replace(',', ''),
                            'Temporary lodging': items[9].text.replace(',', ''),
                            'Meals and incidentals': items[10].text.replace(',', ''),
                            'Childcare': items[11].text.replace(',', ''),
                            'Care of a person with a physical or mental incapacity': items[12].text.replace(',', ''),
                            'Disability - related expenses': items[13].text.replace(',', ''),
                            'Remuneration of candidates representatives': items[14].text.replace(',', ''),
                            'Other': items[15].text.replace(',', '')
                            }
                details.append(row_item)
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)

    return details


def get_other_detail():
    details = []
    browser.find_element_by_id('SelectedPart').click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for option in options:
        if 'Part 3d' in option.text:
            option.click()
            break
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    try:
        rows = browser.find_elements_by_tag_name('tr')
        for r in rows:
            try:
                items = r.find_elements_by_tag_name('td')
                date = items[1].text
                dt_object = datetime.strptime(date, '%b %d, %Y')
                date = dt_object.strftime("%Y-%m-%d")
                row_item = {'date': date,
                            'ED code': items[2].text,
                            'supplier': items[3].text,
                            'description': items[4].text,
                            'Cheque no.': items[5].text,
                            'Amount paid': items[6].text.replace(',', ''),
                            'Contributed or transferred property or service':
                                items[6].text.replace(',', ''),
                            'Unpaid claim': items[7].text.replace(',', ''),
                            'Advances': items[8].text.replace(',', ''),
                            'Transfers': items[9].text.replace(',', ''),
                            'Principal payments on loan': items[10].text.replace(',', ''),
                            'Other': items[11].text.replace(',', '')
                            }
                details.append(row_item)
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)

    return details


def get_non_monetary_transfers_sent_to_political_entities():
    details = []
    browser.find_element_by_id('SelectedPart').click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for option in options:
        if 'Part 3f' in option.text:
            option.click()
            break
        elif 'Part 4 - Statement of Non-Monetary' in option.text:
            option.click()
            break
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    try:
        rows = browser.find_elements_by_tag_name('tr')
        for r in rows:
            try:
                items = r.find_elements_by_tag_name('td')
                date = items[1].text
                dt_object = datetime.strptime(date, '%b %d, %Y')
                date = dt_object.strftime("%Y-%m-%d")
                row_item = {'date': date,
                            'ED code': items[2].text,
                            'Name of political entity receiving transfer': items[3].text,
                            'description': items[4].text,
                            'Nomination contestant Non-monetary': items[5].text.replace(',', ''),
                            'Registered association Non-monetary': items[6].text.replace(',', ''),
                            'Registered party Non-monetary':
                                items[6].text.replace(',', '')
                            }
                details.append(row_item)
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)

    return details


def get_unpaid_claims_detail():
    details = []
    browser.find_element_by_id('SelectedPart').click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for option in options:
        if 'Part 5' in option.text:
            option.click()
            break
        elif 'Part 3e' in option.text:
            option.click()
            break
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    try:
        rows = browser.find_elements_by_tag_name('tr')
        for r in rows:
            try:
                items = r.find_elements_by_tag_name('td')
                date = items[1].text
                dt_object = datetime.strptime(date, '%b %d, %Y')
                date = dt_object.strftime("%Y-%m-%d")
                claim = {'date received': date,
                         'supplier or lender': items[2].text,
                         'unpaid claim': items[3].text.replace(',', ''),
                         'Unpaid claim subject of legal proceedings': items[4].text.replace(',', ''),
                         'Unpaid overdraft or line of credit': items[5].text.replace(',', ''),
                         'Unpaid overdraft or line of credit subject of legal proceedings': items[6].text.replace(',', ''),
                         'Unpaid loan': items[7].text.replace(',', ''),
                         'Unpaid loan subject of legal proceedings': items[7].text.replace(',', '')
                        }
                details.append(claim)
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)

    return details


def get_candidate_election_details(candidate_list):
    completed_list = []
    for c in candidate_list:
        try:
            party = c['party_district'].split('/')[0]
            party_id = get_party_id(party)
            name = c['name']
            gov_id = get_goverlytics_id(name, party_id)
            election_id = get_election_id(c['election'])
            candidate_election_id = get_candidate_election_id(gov_id, party_id, election_id)
            candidate = {'candidate_election_id': candidate_election_id, 'date_of_return': c['date_of_return']}
            election_finances_id = get_candidate_election_finances_id(candidate)
            c.update({'election_finances_id': election_finances_id})
            print(c)
            completed_list.append(c)
        except Exception as e:
            print(e)
    return completed_list


def get_candidate_election_id(gov_id, party_id, election_id):
    if pd.notna(gov_id):
        df = candidates_election
        try:
            ce_id = df.loc[(df["candidate_id"] == gov_id) & (df["party_id"] == party_id) &
                           (df["election_id"] == election_id)]['id'].values[0]
        except:
            ce_id = 0
    try:
        return int(ce_id)
    except Exception:
        return 0


def get_goverlytics_id(name, party):
    last_name = name.split(',')[0].capitalize()
    first_name = name.split(', ')[1].capitalize()
    if pd.notna(name):
        df = candidates_table
        try:
            recipient_id = df.loc[(df["name_first"].str.contains(first_name)) & (df["name_last"] == last_name) &
                                  (df["current_party_id"] == party)]['goverlytics_id'].values[0]
        except:
            recipient_id = 0
    try:
        return int(recipient_id)
    except Exception:
        return 0


def get_party_id(party):
    try:
        party = party.split(' Party')[0]
        party = party.strip()
        party_conversions = {
            "No Affiliation": 'Non-affiliated',
            'Canadian Action': 'Action',
            'Progressive Conservative': 'Conservative',
            'N.D.P.': 'New Democratic',
            'Canadian Reform Conservative Alliance': 'Reform Conservative Alliance',
            'C.H.P. of Canada': 'Christian Heritage',
            'Canadian Alliance': 'Alliance',
            'Independant': 'Independent',
            'Canada': 'Canada Party',
            'The Green': 'Green',
            'Parti Rhinocéros': 'Rhinoceros'
        }
        if party_conversions.get(party):
            party = party_conversions.get(party)
        if pd.notna(party):
            df = scraper_utils.parties
            try:
                value = df.loc[df["party"] == party]['id'].values[0]
                return int(value)
            except Exception as e:
                print(e)
                return 0
    except:
        return 0


def get_election_id(election):
    try:
        if 'by-election' in election.lower():
            election = election.split(' (')[0]
            try:
                date = dparser.parse(election, fuzzy=True)
                date_name = date.strftime("%Y-%m-%d")
                election_name = 'by_election_' + date_name
            except Exception as e:
                print(e)
            if pd.notna(election):
                df = elections
                value = df.loc[df['election_name'].str.contains(election_name)]['id'].values[0]
                try:
                    return int(value)
                except Exception as e:
                    return value
        if 'general' in election.lower():
            general_elections = {
                '50th': '50',
                '49th': '49',
                '48th': '48',
                '47th': '47',
                '46th': '46',
                '45th': '45',
                '44th': '44',
                '43rd': '43',
                '42nd': '42',
                '41st': '41',
                '40th': '40',
                '39th': '39',
                '38th': '38',
                '37th': '37',
                '36th': '36'
            }
            election = election.split(' ')[0].lower()
            e_number = general_elections.get(election)
            election_name = e_number + '_general_election'
            if pd.notna(election):
                df = elections
                value = df.loc[df['election_name'].str.contains(election_name)]['id'].values[0]
                try:
                    return int(value)
                except Exception as e:
                    return value
    except:
        return 0


def get_row_data(data):
    row = scraper_utils.initialize_row()
    row.candidate_election_finances_id = data['election_finances_id']
    row.expenses_limit = data['expenses_limit']
    row.total_expenses_subject_to_limit = data['total_expenses_subject_to_limit']
    row.total_expenses_subject_to_limit_detail = data['total_expenses_subject_to_limit_detail']
    row.personal_expenses = data['personal_expenses']
    row.personal_expenses_detail = data['personal_expenses_detail']
    row.other_expenses = data['other_expenses']
    row.other_detail = data['other_detail']
    row.campaign_expenses = data['campaign_expenses']
    row.contributed_transferred_property_or_service = data['contributed_transferred_property_or_service']
    row.non_monetary_transfers_sent_to_political_entities = data['non_monetary_transfers_sent_to_political_entities']
    row.unpaid_claims = data['unpaid_claims']
    row.unpaid_claims_detail = data['unpaid_claims_detail']
    row.total_outflows = data['total_outflows']
    return row


if __name__ == '__main__':
    data = get_data()
    #print(data)
    candidates_with_no_id_removed = [i for i in data if not (i['election_finances_id'] == 0)]
    row_data = [get_row_data(d) for d in candidates_with_no_id_removed]
    print(row_data)
    scraper_utils.write_data(row_data)
    print('finished')
