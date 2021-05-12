import unicodedata
from pprint import pprint
import re
from datetime import datetime

REGULAR_SESSION_2021 = 2100

SENATE = 'S'
HOUSE = 'H'

SENATE_RESOLUTION = 'SR'

ENROLLED = 'SEC OF STATE'

SENATE_DOMAIN = 'oksenate'
HOUSE_DOMAIN = 'okhouse'

class OKLegislationUtils:
    def get_session(session_code):
        if session_code == REGULAR_SESSION_2021:
            return '2021 Regular Session'

    def get_chamber(chamber_code):
        if chamber_code == SENATE:
            return 'Senate'
        elif chamber_code == HOUSE_CHAMBHOUSEER:
            return 'House'

    def get_bill_type(bill_code):
        if bill_code == SENATE_RESOLUTION:
            return 'Resolution'

    def get_status(status_code):
        if status_code == ENROLLED:
            return 'Enrolled, filed with Secretary of State'

    def get_sponsor_role_from_abbr(sponsor_role_abbr):
        if sponsor_role_abbr == SENATE:
            return 'Senator'
        elif sponsor_role_abbr == HOUSE:
            return 'Representative'

    def get_sponsor_role_from_url(sponsor_url):
        if SENATE_DOMAIN in sponsor_url:
            return 'Senator'
        elif HOUSE_DOMAIN in sponsor_url:
            return 'Representative'

class OKHouseLegislation:
    def _format_votes(soup):
        def _check_and_toggle_voter_flag(page, voter_flag, match_word):
            if match_word in page[i] and page[i + 1] == ' ':
                voter_flag = True
                print(f'START MATCHING {match_word} VOTERS')
            
            return voter_flag

        def _format_voters(voters_list):
            voters_list = ' '.join(voters_list).strip()
            voters_list = re.sub('\s{2,}', ',', voters_list)
            voters_list = voters_list.split(',')

            return voters_list

        text_list = soup.find_all('p', {'class': 'MsoPlainText'})

        text_list = [unicodedata.normalize('NFKD', text.text) for text in text_list]
        text_list = [text.replace('\r\n', ' ') for text in text_list]

        vote_data = {}

        idx_of_page_starts = []

        for idx, text in enumerate(text_list):
            if 'Top_of_Page' in text:
                idx_of_page_starts.append(idx)

        # pprint(idx_of_page_starts)

        page = text_list[idx_of_page_starts[0]:idx_of_page_starts[1] - 1]

        pprint(page, width=200)

        print('BEGIN MATCHING')

        yeas_voter_flag = False
        nays_voter_flag = False
        excused_voter_flag = False
        cp_voter_flag = False

        yeas_voters = []
        nays_voters = []
        excused_voters = []
        cp_voters = []

        for i, text in enumerate(page):
            # Match Yeas
            t = re.search('YEAS:\s+([0-9]+)\s+[A-Z]', text)
            if t != None:
                # print(t.group(1))
                vote_data['yea'] = int(t.group(1))

            # Match Nays
            t = re.search('NAYS:\s+([0-9]+)\s+[0-9]', text)
            if t != None:
                # print(t.group(1))
                vote_data['nay'] = int(t.group(1))

            # Match date
            t = re.search('[0-9]+[/][0-9]+[/][0-9]+', text)
            if t != None:
                # print(t.group(0))
                vote_data['date'] = datetime.strptime(t.group(0), '%m/%d/%Y')

            # Add Yeas voters
            if yeas_voter_flag == True:
                if page[i + 1] != ' ':
                    yeas_voters.append(text)
                else:
                    yeas_voter_flag = False

            # Add Nays voters
            if nays_voter_flag == True:
                if page[i + 1] != ' ':
                    nays_voters.append(text)
                else:
                    nays_voter_flag = False

            # Add Excused voters
            if excused_voter_flag == True:
                if page[i + 1] != ' ':
                    excused_voters.append(text)
                else:
                    excused_voter_flag = False

            # Add Constitutional Privilege voters
            if cp_voter_flag == True:
                if page[i + 1] != ' ':
                    cp_voters.append(text)
                else:
                    cp_voter_flag = False
            
            # Toggle Yeas Flag on
            yeas_voter_flag = _check_and_toggle_voter_flag(page, yeas_voter_flag, 'YEAS')

            # Toggle Nays Flag on
            nays_voter_flag = _check_and_toggle_voter_flag(page, nays_voter_flag, 'NAYS')

            # Toggle Excused Flag on
            excused_voter_flag = _check_and_toggle_voter_flag(page, excused_voter_flag, 'EXCUSED')

            # Toggle CP Flag on
            cp_voter_flag = _check_and_toggle_voter_flag(page, cp_voter_flag, 'CONSTITUIONAL PRIVILEGE')

        # Format voters list
        yeas_voters = _format_voters(yeas_voters)
        nays_voters = _format_voters(nays_voters)
        excused_voters = _format_voters(excused_voters)
        cp_voters = _format_voters(cp_voters)

        pprint(yeas_voters, width=200)
        pprint(nays_voters, width=200)
        pprint(excused_voters, width=200)
        pprint(cp_voters, width=200)

        pprint(vote_data)

        # pprint(text, width=200)
        '''
        [{
            date: 2020-03-23,
            description: On passage of the bill.,
            yea: 123,
            nay: 3,
            nv: 0,
            absent: 1,
            total: 127,
            passed: 1,
            chamber: House,
            votes: [
                {goverlytics_id: 123, legislator: Smith, votet: yea},
                {goverlytics_id: 53, legislator: Johnson, votet: nay}
            ]
        }]
        '''