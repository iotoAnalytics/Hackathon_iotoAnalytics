# TODO - Finish other parser type
# TODO - Refactor
# CP,Vacant = No Vote, Excused = Absent

from scraper_utils import USStateLegislationScraperUtils
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


STATE_ABBREVIATION = 'OK'
DATABASE_TABLE_NAME = 'us_ok_legislation_test'
LEGISLATOR_TABLE_NAME = 'us_ok_legislators_test'

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION, DATABASE_TABLE_NAME, LEGISLATOR_TABLE_NAME)

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
    def _divide_into_pages(self, text):
        pages = []

        first_line_of_page = 0
        last_line_of_page = None

        for idx, line in enumerate(text):
            if 'Top_of_Page' in line:
                # If first line has been set, this means that this is first line of another page
                if first_line_of_page != 0:
                    last_line_of_page = idx - 1
                    page_text = text[first_line_of_page + 1: last_line_of_page] # Add 1 to FLOP to start at the title of page
                    pages.append(page_text)
                
                first_line_of_page = idx + 1

        # Add last page
        last_line_of_page = len(text) - 1
        page_text = text[first_line_of_page + 1: last_line_of_page]
        pages.append(page_text)

        return pages

    # MUST START WITH - OKLAHOMA HOUSE OF REPRESENTATIVES
    def _get_first_line_of_sections(self, page):
        first_line_of_sections = []

        # pprint(page)
        for i, line in enumerate(page[1:]):
            curr_line = page[i]
            prev_line = page[i - 1]

            if curr_line.isspace() == False and prev_line.isspace() == True:
                # print(curr_line)
                first_line_of_sections.append(curr_line)

        # pprint(first_line_of_sections, width=200)

        return first_line_of_sections


    def _parse_house_votes(self, page, first_line_of_sections):
        def _divide_into_sections(first_line_of_sections):
            sections_dict = {
                'title': None,
                'description': None,
                'status': None,
                'vote_outcome': None,
                'yeas_count': None,
                'yeas_voters': [],
                'nays_count':  None,
                'nays_voters': [],
                'exc_count': None,
                'exc_voters': [],
                'cp_count': None,
                'cp_voters': [],
            }

            for i, line in enumerate(first_line_of_sections):
                if 'OKLAHOMA HOUSE OF REPRESENTATIVES' in line:
                    sections_dict['title'] = line
                    sections_dict['description'] = first_line_of_sections[i + 1]
                    sections_dict['status'] = first_line_of_sections[i + 2]
                    sections_dict['vote_outcome'] = first_line_of_sections[i + 3]

                elif yeas_count:= re.match('\s{4}YEAS:\s+([0-9]+)', line):
                    sections_dict['yeas_count'] = line
                    if yeas_count.group(1) != '0':
                        sections_dict['yeas_voters'] = first_line_of_sections[i + 1]
                        
                elif nays_count:= re.match('\s{4}NAYS:\s+([0-9]+)', line):
                    sections_dict['nays_count'] = line
                    if nays_count.group(1) != '0':
                        sections_dict['nays_voters'] = first_line_of_sections[i + 1]

                elif exc_count:= re.match('\s{4}EXCUSED:\s+([0-9]+)', line):
                    sections_dict['exc_count'] = line
                    if exc_count.group(1) != '0':
                        sections_dict['exc_voters'] = first_line_of_sections[i + 1]

                elif cp_count:= re.match('\s{4}CONSTITUTIONAL PRIVILEGE:\s+([0-9]+)', line):
                    sections_dict['cp_count'] = line
                    if cp_count.group(1) != '0':
                        sections_dict['cp_voters'] = first_line_of_sections[i + 1]

            return sections_dict

        def _get_description(page, idx):
            # First two lines have extra word in the beginning for legislation name and author
            # For example:
            # SB49                Chiropractic; modifying certain fees; broadening
            # Osburn              entities who may administer certain examination.
            
            description = ''
            line_count = 0
            while (line := page[idx]).isspace() != True:
                if line_count < 2:
                    description += (re.sub('\s{4}[a-zA-Z0-9]+\s{2}', '', line).strip() + ' ')
                else:
                    description += (line.strip() + ' ')

                line_count += 1
                idx += 1

            return description.strip()

        def _get_passed(page, idx):
            while (line := page[idx]).isspace() != True:
                passed_search = re.search('PASSED|FAILED', line)

                if passed_search != None:
                    if passed_search.group(0) == 'PASSED':
                        return 1
                    elif passed_search.group(0) == 'FAILED':
                        return 0

                idx += 1

        def _get_date(page, idx):
            while (line := page[idx]).isspace() != True:
                date_search = re.search('[0-9]+[/][0-9]+[/][0-9]+', line)
                idx += 1
                if date_search != None:
                    date = datetime.strptime(date_search.group(0), '%m/%d/%Y')
                    return date

        # vote_type must be all caps
        def _get_vote_count(line, vote_type):
            vote_search = re.search(f'{vote_type}:\s+([0-9]+)', line)
            if vote_search != None:
                return int(vote_search.group(1))

            return 0

        def _get_voters(page, idx, sections_dict, sections_dict_key):
            voters = []

            if sections_dict[sections_dict_key] == None:
                return voters

            while (line := page[idx]).isspace() != True:
                voters.append(line)
                idx += 1
            
            return voters

        def _join_voters_into_list(voters):
            if not voters:
                return voters

            voters_list = ' '.join(voters).strip()
            voters_list = re.sub('\s{2,}', ',', voters_list)
            voters_list = voters_list.split(',')
            return voters_list

        # vote_type must the string to put into votes field of votes ('yea', 'nay')
        def _format_voters_list(voters, vote_type):
            formatted_voters = []
            for voter in voters:
                v = _get_voter_data(voter, 'Representative', vote_type)
                formatted_voters.append(v)

            return formatted_voters

        def _get_voter_data(voter, role, vote):
            voter_data = {}

            voter_data['name_last'] = re.sub('\s\([a-zA-Z]+\)', '', voter)
            voter_data['role'] = role
            voter_data['state'] = 'OK'

            first_initial = None

            t = re.search('\([a-zA-Z]+\)', voter)
            if t != None:
                first_initial = re.sub('[\(\)]', '', t.group(0))

            if first_initial != None:
                gov_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', first_initial, **voter_data)
            else:
                if 'Speaker' in voter_data['name_last']:
                    voter_data['name_last'] = scraper_utils.get_attribute('legislator', 'role', 'Speaker', 'name_last') # Presents duplicate issue for future
                    voter_data['role'] = 'Speaker'

                gov_id = scraper_utils.get_legislator_id(**voter_data)

            return {
                'goverlytics_id': gov_id,
                'legislator': voter_data['name_last'],
                'votet': vote
            }


        CHAMBER = 'House'

        vote_data = {}

        vote_data['Chamber'] = CHAMBER

        yeas_voters = []
        nays_voters = []
        exc_voters = []
        cp_voters = []
        sections_dict = _divide_into_sections(first_line_of_sections)

        for i, line in enumerate(page):
            if line == sections_dict['description']:
                vote_data['description'] = _get_description(page, i)

            elif line == sections_dict['status']:
                vote_data['passed'] = _get_passed(page, i)

            elif line == sections_dict['vote_outcome']:
                vote_data['date'] = _get_date(page, i)

            elif line == sections_dict['yeas_count']:
                vote_data['yea'] = _get_vote_count(line, 'YEAS')

            elif line == sections_dict['yeas_voters']:
                yeas_voters = _get_voters(page, i, sections_dict, 'yeas_voters')

            elif line == sections_dict['nays_count']:
                vote_data['nay'] = _get_vote_count(line, 'NAYS')

            elif line == sections_dict['nays_voters']:
                nays_voters = _get_voters(page, i, sections_dict, 'nays_voters')

            elif line == sections_dict['exc_count']:
                vote_data['absent'] = _get_vote_count(line, 'EXCUSED')

            elif line == sections_dict['exc_voters']:
                exc_voters = _get_voters(page, i, sections_dict, 'exc_voters')

            elif line == sections_dict['cp_count']:
                vote_data['nv'] = _get_vote_count(line, 'CONSTITUIONAL PRIVILEGE')

            elif line == sections_dict['cp_voters']:
                cp_voters = _get_voters(page, i, sections_dict, 'cp_voters')


        # pprint(sections_dict, width=200)

        yeas_voters = _join_voters_into_list(yeas_voters)
        nays_voters = _join_voters_into_list(nays_voters)
        exc_voters = _join_voters_into_list(exc_voters)
        cp_voters = _join_voters_into_list(cp_voters)

        formatted_yeas_voters = _format_voters_list(yeas_voters, 'yea')
        formatted_nays_voters = _format_voters_list(nays_voters, 'nay')
        formatted_exc_voters = _format_voters_list(exc_voters, 'absent')
        formatted_cp_voters = _format_voters_list(cp_voters, 'nv')

        all_votes = formatted_yeas_voters + formatted_nays_voters + formatted_exc_voters + formatted_cp_voters
        vote_data['votes'] = all_votes

        all_votes_count = vote_data['yea'] + vote_data['nay'] + vote_data['nv'] + vote_data['absent']
        vote_data['total'] = all_votes_count

        return vote_data
        # pprint(vote_data, width=200)

        # pprint(formatted_yeas_voters, width=200)


    def _parse_page(self, page):
        OK_HOUSE = 0
        OK_HOUSE_COMMITTEE = 1

        votes = []

        # Divide page into parseable sections
        first_line_of_sections = self._get_first_line_of_sections(page)
        # print('First line of sections:')
        # pprint(first_line_of_sections, width=200)

        # Get title of page
        title = first_line_of_sections[0]

        # Determine parse type and parse accordingly
        if re.search('OKLAHOMA HOUSE OF REPRESENTATIVES', title):
            votes = self._parse_house_votes(page, first_line_of_sections)
        elif re.search('Committee', title):
            print('Use Committee Parser')

        return votes


    def format_votes(self, soup):
        vote_data = []

        # Normalize text into readable text
        text_list = soup.find_all('p', {'class': 'MsoPlainText'})
        text_list = [unicodedata.normalize('NFKD', text.text) for text in text_list]
        text_list = [text.replace('\r\n', ' ') for text in text_list]

        # Divide the file into pages
        pages = self._divide_into_pages(text_list)

        for page in pages:
            vote_data.append(self._parse_page(page))

        pprint(vote_data, width=200)

        return vote_data

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

class OKHouseCommitteeParser:
    def _divide_into_sections(self, page):
        sections_dict = {
            'title': None,
            'description': None,
            'status': None, # Note that status is directly after description with no line space in between
            'vote_outcome': None,
            'yeas_count': None,
            'yeas_voters': [],
            'nays_count':  None,
            'nays_voters': [],
            'cp_count': None,
            'cp_voters': [],
        }
        pass

    def parse_votes(self, page, first_line_of_sections):
        sections_dict = _divide_into_sections(first_line_of_sections)
        pass

class OKSenateParser:
    def _divide_into_sections(self, page):
        sections_dict = {
            'title': None,
            'description': None,
            'status': None,
            'vote_outcome': None,
            'yeas_count': None,
            'yeas_voters': [],
            'nays_count':  None,
            'nays_voters': [],
            'exc_count': None,
            'exc_voters': [],
            'nv_count': None,
            'nv_voters': [],
            'cp_count': None,
            'cp_voters': [],
        }
        pass

    def parse_votes(self, page, first_line_of_sections):
        sections_dict = _divide_into_sections(first_line_of_sections)
        pass

class OKSenateCommitteeParser:
    def _divide_into_sections(self, page):
        sections_dict = {
            'title': None,  # Has a blank line next
            'date': None,
            'description': None,    # Title
            'status': None, # Recommendation
            'yeas_voters': [], # Need to count manually
            'nays_voters': [],
            'cp_voters': [],
        }
        pass

    def parse_votes(self, page, first_line_of_sections):
        sections_dict = _divide_into_sections(first_line_of_sections)
        pass
