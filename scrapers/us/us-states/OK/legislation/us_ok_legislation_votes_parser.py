# TODO - Can't handle two word first name (Jo Anne) or ' in name (O'Donnell)
# TODO - Senate Bills doesn't have a status for pass (what's a FAIL condition)
# TODO - Doesn't support House Committee votes with two lines of status (DO PASS AS AMENDED...) 

from scraper_utils import USStateLegislationScraperUtils
import unicodedata
from pprint import pprint
import re
from datetime import datetime

class OKLegislationVotesParser:
    """
    Base class containing common methods and attributes used by all
    Oklahoma legislation vote parsers. 

    Notes:
        constitutional privilege and vacant votes are considered no votes
        and excused votes are considered absent
    """

    STATE_ABBREVIATION = 'OK'
    DATABASE_TABLE_NAME = 'us_ok_legislation_test'
    LEGISLATOR_TABLE_NAME = 'us_ok_legislators_test'

    scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION, DATABASE_TABLE_NAME, LEGISLATOR_TABLE_NAME)

    def get_votes_data(self, soup):
        vote_data = []

        # Get soup
        text_list = soup.find_all('p', {'class': ['MsoPlainText', 'MsoNormal', 'MsoBodyTextIndent']})

        # Normalize text into readable text
        text_list = self.__normalize_text(text_list)

        # Divide the file into pages
        pages = self.__divide_into_pages(text_list)

        # Collect vote data from each page
        for page in pages:
            vote_data.append(self.__parse_page(page))

        return vote_data

    def __normalize_text(self, text_list_soup):
        # Normalizes .HTM text for parsing
        text_list = [unicodedata.normalize('NFKD', text.text) for text in text_list_soup]
        text_list = [text.replace('\r\n', ' ') for text in text_list]
        return text_list

    def __divide_into_pages(self, text):
        # Divides the document into pages
        pages = []

        first_line_of_page = None
        last_line_of_page = None

        for idx, line in enumerate(text):
            if 'Top_of_Page' in line:
                if first_line_of_page != None:
                    last_line_of_page = idx - 2 # Skip Stars line
                    page_text = text[first_line_of_page:last_line_of_page]
                    pages.append(page_text)

                first_line_of_page = idx + 1 # Start after Top_of_Page
        else:
            # Add last page
            last_line_of_page = len(text) - 1
            page_text = text[first_line_of_page:last_line_of_page]
            pages.append(page_text)

        return pages

    def __parse_page(self, page):
        # Parse page and return proper votes format
        votes = []

        # Divide page into parseable sections
        first_line_of_sections = self.__get_first_line_of_sections(page)

        # Get title of page
        title = first_line_of_sections[0]
        line_after_title = first_line_of_sections[1]

        # Determine parse type and parse accordingly
        if 'OKLAHOMA HOUSE OF REPRESENTATIVES' in title:
            votes = OKHouseVotesParser().parse_votes(page, first_line_of_sections)

        elif 'Committee' in title:
            votes = OKHouseCommitteeVotesParser().parse_votes(page, first_line_of_sections)

        elif 'THE OKLAHOMA STATE SENATE' in title and 'Legislature' not in line_after_title:
            votes = OKSenateVotesParser().parse_votes(page, first_line_of_sections)

        elif 'THE OKLAHOMA STATE SENATE' in title:
            votes = OKSenateCommitteeVotesParser().parse_votes(page, first_line_of_sections)

        return votes

    def __get_first_line_of_sections(self, page):
        # Gets the first line of each sections of the page.
        first_line_of_sections = []

        for i, line in enumerate(page[1:]):
            curr_line = page[i]
            prev_line = page[i - 1]

            if curr_line.isspace() == False and prev_line.isspace() == True:
                first_line_of_sections.append(curr_line)

        return first_line_of_sections

    def _get_description(self, page, idx, is_bad_break=False):
        # Gets description of the bill.
        # Usually formatted similar to the following:
        #    THIRD               PASSED
        #    READING

        description = ''
        line = page[idx]

        padding = '' if is_bad_break else ' '

        while not line.isspace():
            if not is_bad_break or 'DO PASS' in line:
                match = re.search('[a-zA-Z0-9]+(\s[a-zA-Z0-9]+)*', line)
                if match:
                    formatted_line = match.group(0).strip() + padding
                    description += formatted_line

            idx += 1
            line = page[idx]

        # Remove extra word (PASSED)
        description = description.replace('PASSED', '').strip()

        return description

    def _get_passed(self, page, idx):
        while not (line := page[idx]).isspace():
            if 'PASSED' in line:
                return 1
                
            idx += 1

    def _get_date(self, page, idx):
        while (line := page[idx]).isspace() != True:
            date_search = re.search('[0-9]+[/][0-9]+[/][0-9]+', line)
            idx += 1
            if date_search != None:
                date = datetime.strptime(date_search.group(0), '%m/%d/%Y')
                return date

    def _get_vote_count(self, line, vote_type):
        vote_search = re.search(f'{vote_type}:\s+([0-9]+)', line)
        if vote_search != None:
            return int(vote_search.group(1))

    def _get_voters(self, page, idx, sections_dict, sections_dict_key):
        voters = []

        if sections_dict[sections_dict_key] == None:
            return voters

        while (line := page[idx]).isspace() != True:
            voters.append(line + ' ')
            idx += 1
        
        return voters

    def _join_voters_into_list(self, voters):
        # Combines lines of voters into a list of voters 
        if not voters:
            return voters

        voters_list = ' '.join(voters).strip()
        voters_list = re.sub('\s{2,}', '|', voters_list)
        voters_list = voters_list.split('|')

        return voters_list

    def _format_voters_list(self, voters, voter_role, vote_type):
        # Formats voters list for the votes field of votes
        formatted_voters = []
        for voter in voters:
            v = self._get_voter_data(voter, voter_role, vote_type)
            formatted_voters.append(v)

        return formatted_voters

    def _get_voter_data(self, voter, role, vote):
        # Returns each voter in the form of: {'goverlytics_id': 34860, 'legislator': 'Hill', 'votet': 'yea'}
        voter_data = {}

        # Dossett (J.J.) or Dossett, J.J.
        voter_data['name_last'] = re.sub('\s\([a-zA-Z\.]+\)|\,\s[a-zA-Z\.]+', '', voter)
        voter_data['role'] = role
        voter_data['state'] = 'OK'

        starts_with = re.search('\([a-zA-Z\.]+\)|\,\s[a-zA-Z\.]+', voter)

        if starts_with != None:
            starts_with = re.sub('[\(\)]|\,\s', '', starts_with.group(0))

            # Special case for Senator Jo Anne Dossett
            if starts_with == 'J.A.':
                starts_with = 'Jo'

            gov_id = self.scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', starts_with, **voter_data)

        else:
            # Special case for Mr/Mrs.Speaker
            # TODO - Presents duplicate issue for speaker
            if 'Speaker' in voter_data['name_last']:
                voter_data['name_last'] = self.scraper_utils.get_attribute('legislator', 'role', 'Speaker', 'name_last')
                voter_data['role'] = 'Speaker'
            
            # Special case for O'Donnell 
            if 'ODonnell' in voter_data['name_last']:
                voter_data['name_last'] = 'O\'Donnell'

            gov_id = self.scraper_utils.get_legislator_id(**voter_data)

        return {
            'goverlytics_id': gov_id,
            'legislator': voter_data['name_last'],
            'votet': vote
        }

class OKHouseVotesParser(OKLegislationVotesParser):
    CHAMBER = 'House'
    ROLE = 'Representative'

    def parse_votes(self, page, first_line_of_sections):
        vote_data = {}

        vote_data['Chamber'] = self.CHAMBER

        yeas_voters = []
        nays_voters = []
        exc_voters = []
        cp_voters = []

        sections_dict = self._divide_into_sections(first_line_of_sections)

        for i, line in enumerate(page):
            if line == sections_dict['description']:
                vote_data['description'] = self._get_description(page, i)

            elif line == sections_dict['status']:
                vote_data['passed'] = self._get_passed(page, i)

            elif line == sections_dict['vote_outcome']:
                vote_data['date'] = self._get_date(page, i)

            elif line == sections_dict['yeas_count']:
                vote_data['yea'] = self._get_vote_count(line, 'YEAS')

            elif line == sections_dict['yeas_voters']:
                yeas_voters = self._get_voters(page, i, sections_dict, 'yeas_voters')

            elif line == sections_dict['nays_count']:
                vote_data['nay'] = self._get_vote_count(line, 'NAYS')

            elif line == sections_dict['nays_voters']:
                nays_voters = self._get_voters(page, i, sections_dict, 'nays_voters')

            elif line == sections_dict['exc_count']:
                vote_data['absent'] = self._get_vote_count(line, 'EXCUSED')

            elif line == sections_dict['exc_voters']:
                exc_voters = self._get_voters(page, i, sections_dict, 'exc_voters')

            elif line == sections_dict['cp_count']:
                vote_data['nv'] = self._get_vote_count(line, 'CONSTITUTIONAL PRIVILEGE')

            elif line == sections_dict['cp_voters']:
                cp_voters = self._get_voters(page, i, sections_dict, 'cp_voters')

        yeas_voters = self._join_voters_into_list(yeas_voters)
        nays_voters = self._join_voters_into_list(nays_voters)
        exc_voters = self._join_voters_into_list(exc_voters)
        cp_voters = self._join_voters_into_list(cp_voters)

        formatted_yeas_voters = self._format_voters_list(yeas_voters, self.ROLE, 'yea')
        formatted_nays_voters = self._format_voters_list(nays_voters, self.ROLE, 'nay')
        formatted_exc_voters = self._format_voters_list(exc_voters, self.ROLE, 'absent')
        formatted_cp_voters = self._format_voters_list(cp_voters, self.ROLE, 'nv')

        all_votes = formatted_yeas_voters + formatted_nays_voters + formatted_exc_voters + formatted_cp_voters
        vote_data['votes'] = all_votes

        all_votes_count = vote_data['yea'] + vote_data['nay'] + vote_data['absent'] + vote_data['nv']
        vote_data['total'] = all_votes_count

        return vote_data

    def _divide_into_sections(self, first_line_of_sections):
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
                sections_dict['description'] = first_line_of_sections[i + 2]
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

class OKHouseCommitteeVotesParser(OKLegislationVotesParser):
    CHAMBER = 'House'
    ROLE = 'Representative'

    def parse_votes(self, page, first_line_of_sections):
        vote_data = {}

        vote_data['chamber'] = self.CHAMBER
        vote_data['absent'] = 0

        yeas_voters = []
        nays_voters = []
        cp_voters = []
        
        sections_dict = self._divide_into_sections(first_line_of_sections)

        for i, line in enumerate(page):
            if line == sections_dict['description']:
                vote_data['description'] = self._get_description(page, i, True)
                vote_data['passed'] = self._get_passed(page, i)

            elif line == sections_dict['vote_outcome']:
                vote_data['date'] = self._get_date(page, i)

            elif line == sections_dict['yeas_count']:
                vote_data['yea'] = self._get_vote_count(line, 'YEAS')

            elif line == sections_dict['yeas_voters']:
                yeas_voters = self._get_voters(page, i, sections_dict, 'yeas_voters')

            elif line == sections_dict['nays_count']:
                vote_data['nay'] = self._get_vote_count(line, 'NAYS')

            elif line == sections_dict['nays_voters']:
                nays_voters = self._get_voters(page, i, sections_dict, 'nays_voters')

            elif line == sections_dict['cp_count']:
                vote_data['nv'] = self._get_vote_count(line, 'CONSTITUTIONAL PRIVILEGE')

            elif line == sections_dict['cp_voters']:
                cp_voters = self._get_voters(page, i, sections_dict, 'cp_voters')

        yeas_voters = self._join_voters_into_list(yeas_voters)
        nays_voters = self._join_voters_into_list(nays_voters)
        cp_voters = self._join_voters_into_list(cp_voters)

        formatted_yeas_voters = self._format_voters_list(yeas_voters, self.ROLE, 'yea')
        formatted_nays_voters = self._format_voters_list(nays_voters, self.ROLE, 'nay')
        formatted_cp_voters = self._format_voters_list(cp_voters, self.ROLE, 'nv')

        all_votes = formatted_yeas_voters + formatted_nays_voters + formatted_cp_voters
        vote_data['votes'] = all_votes

        all_votes_count = vote_data['yea'] + vote_data['nay'] + vote_data['nv']
        vote_data['total'] = all_votes_count

        return vote_data

    def _divide_into_sections(self, first_line_of_sections):
        sections_dict = {
            'title': None,
            'description': None,
            'vote_outcome': None,
            'yeas_count': 0,
            'yeas_voters': [],
            'nays_count':  0,
            'nays_voters': [],
            'cp_count': 0,
            'cp_voters': [],
        }

        for i, line in enumerate(first_line_of_sections):
            if 'Committee' in line:
                sections_dict['title'] = line
                sections_dict['description'] = first_line_of_sections[i + 2]
                sections_dict['vote_outcome'] = first_line_of_sections[i + 3]

            elif yeas_count:= re.match('\s{4}YEAS:\s+([0-9]+)', line):
                sections_dict['yeas_count'] = line
                if yeas_count.group(1) != '0':
                    sections_dict['yeas_voters'] = first_line_of_sections[i + 1]
                    
            elif nays_count:= re.match('\s{4}NAYS:\s+([0-9]+)', line):
                sections_dict['nays_count'] = line
                if nays_count.group(1) != '0':
                    sections_dict['nays_voters'] = first_line_of_sections[i + 1]

            elif cp_count:= re.match('\s{4}CONSTITUTIONAL PRIVILEGE:\s+([0-9]+)', line):
                sections_dict['cp_count'] = line
                if cp_count.group(1) != '0':
                    sections_dict['cp_voters'] = first_line_of_sections[i + 1]

        return sections_dict

class OKSenateVotesParser(OKLegislationVotesParser):
    CHAMBER = 'Senate'
    ROLE = 'Senator'

    def parse_votes(self, page, first_line_of_sections):
        vote_data = {}

        vote_data['chamber'] = self.CHAMBER
        vote_data['absent'] = 0
        vote_data['nv'] = 0

        yeas_voters = []
        nays_voters = []
        exc_voters = []
        nv_voters = []
        vac_voters = []

        sections_dict = self._divide_into_sections(first_line_of_sections)

        for i, line in enumerate(page):
            if line == sections_dict['description']:
                vote_data['description'] = self._get_description(page, i)

            # elif line == sections_dict['status']:
            #     vote_data['passed'] = self._get_passed(page, i)

            elif line == sections_dict['vote_outcome']:
                vote_data['date'] = self._get_date(page, i)

            elif line == sections_dict['yeas_count']:
                vote_data['yea'] = self._get_vote_count(line, 'YEAS')

            elif line == sections_dict['yeas_voters']:
                yeas_voters = self._get_voters(page, i, sections_dict, 'yeas_voters')

            elif line == sections_dict['nays_count']:
                vote_data['nay'] = self._get_vote_count(line, 'NAYS')

            elif line == sections_dict['nays_voters']:
                nays_voters = self._get_voters(page, i, sections_dict, 'nays_voters')

            elif line == sections_dict['exc_count']:
                vote_data['absent'] += self._get_vote_count(line, 'EXCUSED')

            elif line == sections_dict['exc_voters']:
                exc_voters = self._get_voters(page, i, sections_dict, 'exc_voters')
        
            elif line == sections_dict['nv_count']:
                vote_data['nv'] += self._get_vote_count(line, 'N/V')

            elif line == sections_dict['nv_voters']:
                nv_voters = self._get_voters(page, i, sections_dict, 'nv_voters')

            elif line == sections_dict['vac_count']:
                vote_data['nv'] += self._get_vote_count(line, 'VACANT')

            elif line == sections_dict['vac_voters']:
                vac_voters = self._get_voters(page, i, sections_dict, 'vac_voters')

        yeas_voters = self._join_voters_into_list(yeas_voters)
        nays_voters = self._join_voters_into_list(nays_voters)
        exc_voters = self._join_voters_into_list(exc_voters)
        nv_voters = self._join_voters_into_list(nv_voters)
        vac_voters = self._join_voters_into_list(vac_voters)

        formatted_yeas_voters = self._format_voters_list(yeas_voters, self.ROLE, 'yea')
        formatted_nays_voters = self._format_voters_list(nays_voters, self.ROLE, 'nay')
        formatted_exc_voters = self._format_voters_list(exc_voters, self.ROLE, 'absent')
        formatted_nv_voters = self._format_voters_list(nv_voters, self.ROLE, 'nv')
        formatted_vac_voters = self._format_voters_list(vac_voters, self.ROLE, 'nv')

        all_votes = formatted_yeas_voters + formatted_nays_voters + formatted_exc_voters + formatted_nv_voters + formatted_vac_voters
        vote_data['votes'] = all_votes

        all_votes_count = vote_data['yea'] + vote_data['nay'] + vote_data['absent'] + vote_data['nv']
        vote_data['total'] = all_votes_count

        return vote_data

    def _divide_into_sections(self, first_line_of_sections):
        sections_dict = {
            'title': None,
            'description': None,
            'vote_outcome': None,
            'yeas_count': 0,
            'yeas_voters': [],
            'nays_count':  0,
            'nays_voters': [],
            'exc_count': 0,
            'exc_voters': [],
            'nv_count': 0,
            'nv_voters': [],
            'vac_count': 0,
            'vac_voters': [],
        }

        for i, line in enumerate(first_line_of_sections):
            if 'THE OKLAHOMA STATE SENATE' in line:
                sections_dict['title'] = line
                sections_dict['description'] = first_line_of_sections[i + 2]
                sections_dict['vote_outcome'] = first_line_of_sections[i + 3]

            elif yeas_count:= re.match('YEAS:\s+([0-9]+)', line):
                sections_dict['yeas_count'] = line
                if yeas_count.group(1) != '0':
                    sections_dict['yeas_voters'] = first_line_of_sections[i + 1]
                    
            elif nays_count:= re.match('NAYS:\s+([0-9]+)', line):
                sections_dict['nays_count'] = line
                if nays_count.group(1) != '0':
                    sections_dict['nays_voters'] = first_line_of_sections[i + 1]

            elif nays_count:= re.match('EXCUSED:\s+([0-9]+)', line):
                sections_dict['exc_count'] = line
                if nays_count.group(1) != '0':
                    sections_dict['exc_voters'] = first_line_of_sections[i + 1]

            elif nays_count:= re.match('N/V:\s+([0-9]+)', line):
                sections_dict['nv_count'] = line
                if nays_count.group(1) != '0':
                    sections_dict['nv_voters'] = first_line_of_sections[i + 1]

            elif cp_count:= re.match('VACANT:\s+([0-9]+)', line):
                sections_dict['vac_count'] = line
                # if cp_count.group(1) != '0':
                #     sections_dict['vac_voters'] = first_line_of_sections[i + 1]

        return sections_dict

class OKSenateCommitteeVotesParser(OKLegislationVotesParser):
    CHAMBER = 'Senate'
    ROLE = 'Senator'

    def parse_votes(self, page, first_line_of_sections):
        vote_data = {}

        vote_data['chamber'] = self.CHAMBER
        vote_data['absent'] = 0

        yeas_voters = []
        nays_voters = []
        cp_voters = []

        sections_dict = self._divide_into_sections(first_line_of_sections)

        for i, line in enumerate(page):
            if line == sections_dict['date']:
                vote_data['data'] = self._get_date(page, i)

            if line == sections_dict['description']:
                vote_data['description'] = self._get_description(page, i, True)

            elif line == sections_dict['status']:
                vote_data['passed'] = self._get_passed(page, i)

            elif line == sections_dict['yeas_voters']:
                yeas_voters = self._get_voters(page, i)
                yeas_voters = self._format_voters_list(yeas_voters, self.ROLE, 'yea')
                vote_data['yea'] = len(yeas_voters)

            elif line == sections_dict['nays_voters']:
                nays_voters = self._get_voters(page, i)
                nays_voters = self._format_voters_list(nays_voters, self.ROLE, 'nay')
                vote_data['nay'] = len(nays_voters)

            elif line == sections_dict['cp_voters']:
                cp_voters = self._get_voters(page, i)
                cp_voters = self._format_voters_list(cp_voters, self.ROLE, 'nv')
                vote_data['nv'] = len(cp_voters)

        all_votes = yeas_voters + nays_voters + cp_voters
        vote_data['votes'] = all_votes

        all_votes_count = vote_data['yea'] + vote_data['nay'] + vote_data['nv']
        vote_data['total'] = all_votes_count

        return vote_data

    def _divide_into_sections(self, first_line_of_sections):
        sections_dict = {
            'title': None,
            'date': None,
            'description': None,
            'status': None,
            'yeas_voters': [],
            'nays_voters': [],
            'cp_voters': [],
        }

        for i, line in enumerate(first_line_of_sections):
            if 'THE OKLAHOMA STATE SENATE' in line and 'Legislature' in first_line_of_sections[i + 1]:
                sections_dict['title'] = line
                sections_dict['date'] = first_line_of_sections[i + 2]
                sections_dict['description'] = first_line_of_sections[i + 3]
                sections_dict['status'] = first_line_of_sections[i + 7]
                sections_dict['yeas_voters'] = first_line_of_sections[i + 8]
                sections_dict['nays_voters'] = first_line_of_sections[i + 9]
                sections_dict['cp_voters'] = first_line_of_sections[i + 10]
                return sections_dict
    
    def _get_date(self, page, idx):
        date = page[idx].strip()
        date = datetime.strptime(date, '%B %d, %Y')
        return date

    def _get_passed(self, page, idx):
        if 'PASS' in page[idx]:
            return 1
        elif 'FAILED' in page[idx]:
            return 0

    def _get_voters(self, page, idx):
        voters = page[idx]
        voters = re.sub('[a-zA-Z0-9]+(\s[a-zA-Z0-9]+)*\:\s+', '', voters)
        voters = [] if len(voters) == 0 else voters.split(', ')
        return voters