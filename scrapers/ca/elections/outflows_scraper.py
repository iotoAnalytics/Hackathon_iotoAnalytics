import sys
import os
from datetime import datetime
from pathlib import Path
import time
from selenium import webdriver
from scraper_utils import InflowScraperUtils
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
scraper_utils = InflowScraperUtils(COUNTRY, TABLE)
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
    browser.find_element_by_xpath('//*[@id="SelectedClientIds"]/option[1]').click()
    #browser.find_element_by_id('SelectAllCandidates').click()
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
            if 'Part  4' in option.text:
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
        print(candidate)
        #contribution_detail = get_contribution_detail()
        unpaid_claims_detail = get_unpaid_claims_detail()
        # if not contribution_detail:
        #     contribution_detail = [{}]
        #print(contribution_detail)
        loans_detail = get_loans_detail()
        # if not loans_detail:
        #     loans_detail = [{}]
        #print(loans_detail)
        returned_detail = get_returned_detail()
        # if not returned_detail:
        #     returned_detail = [{}]
        #print(returned_detail)
        transfer_detail = get_transfer_detail()
        # if not transfer_detail:
        #     transfer_detail = [{}]
        other_inflow_detail = get_other_inflow_detail()
        candidate_info = {'election': election, 'name': candidate, 'date_of_return': date_of_return,
                          'party_district': party_district, 'unpaid claims detail': unpaid_claims_detail,
                          'loans_detail': loans_detail, 'returned_detail': returned_detail,
                          'transfer_detail': transfer_detail, 'other_inflow_detail': other_inflow_detail}
        outflow_data = get_outflow_data()
        #print(candidate_info)
        #candidate_info.update(inflow_data)
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
        if 'Part 3c' in option.text:
            option.click()
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    items = browser.find_elements_by_tag_name('tr')
    for i in items:
        line_header = i.find_element_by_tag_name('th')
        try:
            if 'Election expenses limit' in line_header.text:
                expenses_limit = i.find_element_by_tag_name('td').text.replace(',', '')
                print(expenses_limit)
            if 'Election expenses subject to the limit - Total' in line_header.text:
                total_expenses_subject_to_limit = i.find_element_by_tag_name('td').text.replace(',', '')
                print(total_expenses_subject_to_limit)
            if 'Personal expenses - Total' in line_header.text:
                personal_expenses = i.find_element_by_tag_name('td').text.replace(',', '')
                print(personal_expenses)
            if 'Other expenses and outflows - Total' in line_header.text:
                other_expenses = i.find_element_by_tag_name('td').text.replace(',', '')
                print(other_expenses)
            if 'Total - Unpaid claim' in line_header.text:
                unpaid_claims = i.find_element_by_tag_name('td').text.replace(',', '')
                print(unpaid_claims)
            if 'Grand total' in line_header.text:
                total_outflows = i.find_element_by_tag_name('td').text.replace(',', '')
                print(total_outflows)

            loans_received = get_loans(items)
            transfer_totals = get_transfer_totals(items)
        except:
            pass
    try:
        outflow_data = {
            'expenses_limit': float(expenses_limit),
            'total_expenses_subject_to_limit': float(total_expenses_subject_to_limit),
            'personal_expenses': float(personal_expenses),
            'other_expenses': float(other_expenses),
            'unpaid_claims': float(unpaid_claims),
            'total_outflows': float(total_outflows)
              }
    except:
        outflow_data = {
            'expenses_limit': 0.00,
            'total_expenses_subject_to_limit': 0.00,
            'personal_expenses': 0.00,
            'other_expenses': 0.00,
            'unpaid_claims': 0.00,
            'total_outflows': 0.00
        }
    return outflow_data


def get_transfer_totals(items):
    for i in items:
        line_header = i.find_element_by_tag_name('th')
        if 'Non-monetary transfers from registered party' in line_header.text:
            party = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Non-monetary transfers from registered associations' in line_header.text:
            association = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Total non-monetary transfers' in line_header.text:
            non_monetary = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Monetary transfers from registered party' in line_header.text:
            party2 = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Monetary transfers from registered associations' in line_header.text:
            association2 = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Monetary transfers from nomination contestants' in line_header.text:
            contestants = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Total monetary transfers' in line_header.text:
            total_monetary = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Total transfers' in line_header.text:
            total = i.find_element_by_tag_name('td').text.replace(',', '')

    transfer_totals = { 'Non-monetary transfers from registered party': party,
                        'Non-monetary transfers from registered associations': association,
                        'Total non-monetary transfers': non_monetary,
                        'Monetary transfers from registered party': party2,
                        'Monetary transfers from registered associations': association2,
                        'Monetary transfers from nomination contestants': contestants,
                        'Total monetary transfers': total_monetary,
                        'Total transfers': total
                        }

    return transfer_totals


def get_loans(items):
    for i in items:
        line_header = i.find_element_by_tag_name('th')
        if 'Total loans from individuals' in line_header.text:
            individuals = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Total loans from financial institutions' in line_header.text:
            institutions = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Total loans from registered party' in line_header.text:
            party = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Total loans from registered associations' in line_header.text:
            association = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Total number of lenders - individuals' in line_header.text:
            lender_i = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Total number of lenders - financial institutions' in line_header.text:
            lender_f = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Total number of lenders - registered party' in line_header.text:
            lender_r = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Total number of lenders - registered associations' in line_header.text:
            lender_a = i.find_element_by_tag_name('td').text.replace(',', '')
        if 'Total number of lenders' in line_header.text:
            lenders = i.find_element_by_tag_name('td').text.replace(',', '')

    loans_received = {'loans from individuals': float(individuals),
                      'loans from financial institutions': float(institutions),
                      'loans from registered party': float(party),
                      'loans from registered associations': float(association),
                      'total lenders individuals': int(lender_i),
                      'total lenders financial institutions': int(lender_f),
                      'total lenders registered party': int(lender_r),
                      'total lenders registered associations': int(lender_a),
                      'total lenders': int(lenders)
                      }
    return loans_received


def get_other_inflow_detail():
    details = []
    browser.find_element_by_id('SelectedPart').click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for option in options:
        if 'Part 2i' in option.text:
            option.click()
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    try:
        body = browser.find_element_by_tag_name('tbody')
        items = body.find_elements_by_tag_name('tr')
        for i in items:
            information = i.find_elements_by_tag_name('td')
            date_received = information[1].text
            dt_object = datetime.strptime(date_received, '%b %d, %Y')
            received = dt_object.strftime("%Y-%m-%d")
            description = information[2].text
            non_contribution_portion = information[3].text.replace(',', '')
            bank_interest_earned = information[4].text.replace(',', '')
            refunds_from_suppliers = information[5].text.replace(',', '')
            returned_advances = information[6].text.replace(',', '')
            proceeds = information[7].text.replace(',', '')
            others = information[8].text.replace(',', '')
            other_inflow_detail = {'date received': received,
                        'description': description,
                        'Non-contribution portion of fundraiser': non_contribution_portion,
                        'Bank interest earned': bank_interest_earned,
                        'Refunds from suppliers': refunds_from_suppliers,
                        'Returned portion of advances': returned_advances,
                        'Proceeds of sale from residual assets': proceeds,
                        'others': others
                        }

            details.append(other_inflow_detail)
    except:
        other_inflow_detail = {'date received': None,
                               'description': None,
                               'Non-contribution portion of fundraiser': None,
                               'Bank interest earned': None,
                               'Refunds from suppliers': None,
                               'Returned portion of advances': None,
                               'Proceeds of sale from residual assets': None,
                               'others': None
                               }
        details.append(other_inflow_detail)
    # print(details)
    return details


def get_unpaid_claims_detail():
    details = []
    browser.find_element_by_id('SelectedPart').click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for option in options:
        if 'Part 5' in option.text:
            option.click()
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    try:
        rows = browser.find_elements_by_tag_name('tr')
        for r in rows:

            items = r.find_elements_by_tag_name('td')
            date = items[1].text
            dt_object = datetime.strptime(date, '%b %d, %Y')
            date = dt_object.strftime("%Y-%m-%d")
            claim = {'date received': date,
                     'supplier or lender': items[2].text,
                     'unpaid claim': float(items[3].text),
                     'Unpaid claim subject of legal proceedings': float(items[4].text.replace(',', '')),
                     'Unpaid overdraft or line of credit': float(items[5].text.replace(',', '')),
                     'Unpaid overdraft or line of credit subject of legal proceedings': float(items[6].text.replace(',', '')),
                     'Unpaid loan': float(items[7].text.replace(',', '')),
                     'Unpaid loan subject of legal proceedings': float(items[7].text.replace(',', ''))
                    }
            details.append(claim)
    except:
        pass

    return details


def get_loans_detail():
    #print("get loans detail")
    loan_list = []
    lender_list = []
    detail_list = []
    browser.find_element_by_id('SelectedPart').click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for option in options:
        if 'Part 2b' in option.text:
            option.click()
            break
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    try:
        tables = browser.find_elements_by_tag_name('table')
        # number_of_tables = len(tables)
        # print(number_of_tables)
        for table in tables:
            if 'Lender' in table.text:
                lines = table.find_elements_by_tag_name('tr')
                columns = lines[2].find_elements_by_tag_name('td')
                name_of_lender = columns[0].text
                address = columns[1].text
                type_of_lender = columns[2].text
                lender = {'name of lender': name_of_lender,
                          'address': address,
                          'type of lender': type_of_lender}
                lender_list.append(lender)
            elif 'Loan' in table.text:
                lines = table.find_elements_by_tag_name('tr')
                columns = lines[2].find_elements_by_tag_name('td')
                date = columns[1].text
                type_of_loan = columns[2].text
                fixed_interest = columns[3].text
                variable_interest = columns[4].text
                amount = columns[5].text
                principal_payments = columns[6].text
                interest_payments = columns[7].text
                balance = columns[8].text
                detail = {'date received': date,
                'type of loan': type_of_loan,
                'fixed interest rate': fixed_interest,
                'variable interest rate': variable_interest,
                'amount borrowed': amount,
                'principal payments': principal_payments,
                'interest payments': interest_payments,
                'balance':balance}
                detail_list.append(detail)

        for i in range(0, len(lender_list)):
            #print(lender_list[i])
            lender = lender_list[i]
            loan = detail_list[i]
            lender.update(loan)
            #print(lender)
            loan_list.append(lender)

    except Exception as e:
        print(e)
        detail = {'date received': None,
                  'type of loan': None,
                  'fixed interest rate': None,
                  'variable interest rate': None,
                  'amount borrowed': None,
                  'principal payments': None,
                  'interest payments': None,
                  'balance': None}
        loan_list.append(detail)
    #print(loan_list)
    return loan_list


def get_returned_detail():
    #print("returned detail")
    details = []
    browser.find_element_by_id('SelectedPart').click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for option in options:
        if 'Part 2c' in option.text:
            option.click()
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    try:
        body = browser.find_element_by_tag_name('tbody')
        items = body.find_elements_by_tag_name('tr')
        for i in items:
            information = i.find_elements_by_tag_name('td')
            date_received = information[1].text
            dt_object = datetime.strptime(date_received, '%b %d, %Y')
            received = dt_object.strftime("%Y-%m-%d")
            name = information[3].text
            address = information[4].text
            c_type = information[2].text
            monetary = information[5].text.replace(',','')
            non_monetary = information[6].text.replace(',','')
            date_returned = information[7].text
            dt_object = datetime.strptime(date_returned, '%b %d, %Y')
            returned = dt_object.strftime("%Y-%m-%d")
            ceo = information[8].text
            if ceo:
                dt_object = datetime.strptime(ceo, '%b %d, %Y')
                ceo_date = dt_object.strftime("%Y-%m-%d")
            else:
                ceo_date = ''
            returned = {'date received': received,
                        'name of contributor': name,
                        'address': address,
                        'type of contributor': c_type,
                        'monetary': monetary,
                        'non-monetary': non_monetary,
                        'date returned': returned,
                        'date remitted to CEO': ceo_date
                        }

            details.append(returned)
    except:
        returned = {'date received': None,
                    'name of contributor': None,
                    'address': None,
                    'type of contributor': None,
                    'monetary':  None,
                    'non-monetary': None,
                    'date returned': None,
                    'date remitted to CEO': None
                    }
        details.append(returned)
    #print(details)
    return details


def get_transfer_detail():
    details = []
    browser.find_element_by_id('SelectedPart').click()
    time.sleep(1)
    options = browser.find_elements_by_tag_name('option')
    for option in options:
        if 'Part 2d' in option.text:
            option.click()
    time.sleep(1)
    browser.find_element_by_id('ReportOptions').click()
    time.sleep(1)
    try:
        body = browser.find_element_by_tag_name('tbody')
        items = body.find_elements_by_tag_name('tr')
        for i in items:
            information = i.find_elements_by_tag_name('td')
            date_received = information[1].text
            dt_object = datetime.strptime(date_received, '%b %d, %Y')
            received = dt_object.strftime("%Y-%m-%d")
            name = information[3].text
            party_monetary = information[4].text.replace(',', '')
            ed_code = information[2].text
            party_non_monetary = information[5].text.replace(',', '')
            reg_monetary = information[6].text.replace(',', '')
            reg_non_monetary = information[7].text
            nomination = information[8].text

            transfers = {'Date received': received,
                         'ED code of transferor': ed_code,
                         'Name of political entity making transfer': name,
                         'Registered party monetary': party_monetary,
                         'Registered party non monetary': party_non_monetary,
                         'Registered association monetary': reg_monetary,
                         'Registered association non monetary': reg_non_monetary,
                         'Nomination contestant monetary': nomination
                        }
            details.append(transfers)
    except:
        transfers = {'Date received': None,
                     'ED code of transferor': None,
                     'Name of political entity making transfer': None,
                     'Registered party monetary': None,
                     'Registered party non monetary': None,
                     'Registered association monetary': None,
                     'Registered association non monetary': None,
                     'Nomination contestant monetary': None
                     }
        details.append(transfers)
    #print(details)
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
    row.candidate_election_finances_id = int(data['election_finances_id'])
    row.monetary = float(data['monetary'])
    row.non_monetary = float(data['non_monetary'])
    row.contribution_detail = data['contributions_detail']
    row.contribution_totals = data['contribution_totals']
    row.loans = data['loans']
    row.loans_received = data['loans_received']
    row.loans_detail = data['loans_detail']
    row.monetary_returned = float(data['monetary_returned'])
    row.non_monetary_returned = float(data['non_monetary_returned'])
    row.returned_detail = data['returned_detail']
    row.monetary_transfer_received = float(data['monetary_transfer_received'])
    row.non_monetary_transfer_received = float(data['non_monetary_transfer_received'])
    row.transfer_totals = data['transfer_totals']
    row.transfer_detail = data['transfer_detail']
    row.other_cash_inflow = float(data['other_cash_inflow'])
    row.other_inflow_detail = data['other_inflow_detail']
    row.total_inflow = float(data['total_inflow'])
    return row


if __name__ == '__main__':
    data = get_data()
    #print(data)
    row_data = [get_row_data(d) for d in data]
    print(row_data)
    scraper_utils.write_data(row_data)
    print('finished')
