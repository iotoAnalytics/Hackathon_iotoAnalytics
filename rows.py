from typing import List, Type
from dataclasses import dataclass, field
from datetime import datetime
import re


#########################################################
#       LEGISLATION ROWS                                #
#########################################################
@dataclass
class LegislationRow:
    """
    Data structure for housing data about each piece of legislation.
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value
    goverlytics_id: str
    source_id: str
    bill_name: str
    session: str
    date_introduced: datetime
    source_url: str
    chamber_origin: str
    committees: List[dict]
    bill_type: str
    bill_title: str
    current_status: str
    principal_sponsor_id: int
    principal_sponsor: str
    sponsors: List[str]
    sponsors_id: List[int]
    cosponsors: List[str]
    cosponsors_id: List[int]
    bill_text: str
    bill_description: str
    bill_summary: str
    actions: List[dict] 
    votes: List[dict] 
    source_topic: str
    topic: str
    country_id: int
    country: str

    def __init__(self):
        self._goverlytics_id = ''
        self._source_id = ''
        self._bill_name = ''
        self._session = ''
        self._date_introduced = None
        self._source_url = ''
        self._chamber_origin = ''
        self._committees = []
        self._bill_type = ''
        self._bill_title = ''
        self._current_status = ''
        self._principal_sponsor_id = None
        self._principal_sponsor: str = ''
        self._sponsors = []
        self._sponsors_id = []
        self._cosponsors = []
        self._cosponsors_id = []
        self._bill_text = ''
        self._bill_description = ''
        self._bill_summary = ''
        self._actions = []
        self._votes = []
        self._source_topic = ''
        self._topic = ''
        self._country_id = 0
        self._country = ''

    @property
    def goverlytics_id(self) -> str:
        return self._goverlytics_id
    @goverlytics_id.setter
    def goverlytics_id(self, id: str) -> None:
        if type(id) != str:
            raise TypeError("goverlytics_id must be a str")
        self._goverlytics_id = id

    @property
    def source_id(self) -> str:
        return self._source_id
    @source_id.setter
    def source_id(self, id: str) -> None:
        if type(id) != str:
            raise TypeError("source_id must be a str")
        self._source_id = id

    @property
    def bill_name(self) -> str:
        return self._bill_name
    @bill_name.setter
    def bill_name(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("bill_name must be a str")
        self._bill_name = value

    @property
    def session(self) -> str:
        return self._session
    @session.setter
    def session(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("session must be a str")
        self._session = value

    @property
    def date_introduced(self) -> str:
        return self._date_introduced
    @date_introduced.setter
    def date_introduced(self, value: str) -> None:
        if value and type(value) != str:
            raise TypeError("date_introduced must be a str")
        if value and not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
            raise ValueError("Improper date formating in date_introduced. Required format: YYYY-MM-DD")
        self._date_introduced = value

    @property
    def source_url(self) -> str:
        return self._source_url
    @source_url.setter
    def source_url(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("source_url must be a str")
        if len(value) == 0 or value == None:
            raise ValueError("source_url must not be an empty string")
        self._source_url = value

    @property
    def chamber_origin(self) -> str:
        return self._chamber_origin
    @chamber_origin.setter
    def chamber_origin(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("chamber_origin must be a str")
        self._chamber_origin = value

    @property
    def committees(self) -> List[dict]:
        return self._committees
    @committees.setter
    def committees(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("committees must be a list of dicts")
        for committee in value:
            if type(committee) != dict:
                raise TypeError("committees must be a list of dicts")
            if committee.get('chamber') is None or type(committee.get('chamber')) != str:
                raise ValueError("committees data must have valid 'chamber' information as a str")
            if committee.get('committee') is None or type(committee.get('committee')) != str:
                raise ValueError("committees data must have valid 'committee' information as a str")
        self._committees = value

    @property
    def bill_type(self) -> str:
        return self._bill_type
    @bill_type.setter
    def bill_type(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("bill_type must be a str")
        self._bill_type = value

    @property
    def bill_title(self) -> str:
        return self._bill_title
    @bill_title.setter
    def bill_title(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("bill_title must be a str")
        self._bill_title = value

    @property
    def current_status(self) -> str:
        return self._current_status
    @current_status.setter
    def current_status(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("current_status must be a str")
        self._current_status = value

    @property
    def principal_sponsor_id(self) -> int:
        return self._principal_sponsor_id
    @principal_sponsor_id.setter
    def principal_sponsor_id(self, id: int) -> None:
        if type(id) != int:
            raise TypeError("principal_sponsor_id must be a int")
        self._principal_sponsor_id = id

    @property
    def principal_sponsor(self) -> str:
        return self._principal_sponsor
    @principal_sponsor.setter
    def principal_sponsor(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("principal_sponsor must be a str")
        names_split = value.split(' ')
        if value and len(names_split) == 0:
            raise ValueError("principal_sponsor cannot be empty")
        self._principal_sponsor = value

    @property
    def sponsors(self) -> List[str]:
        return self._sponsors
    @sponsors.setter
    def sponsors(self, value: List[str]) -> None:
        if type(value) != list:
            raise TypeError("sponsors must be a list of strs")
        if not all(isinstance(element, str) for element in value):
            raise TypeError("sponsors must be a list of strs")
        self._sponsors = value

    @property
    def sponsors_id(self) -> List[int]:
        return self._sponsors_id
    @sponsors_id.setter
    def sponsors_id(self, value: List[int]) -> None:
        if type(value) != list:
            raise TypeError("sponsors_id must be a list of ints")
        if not all(isinstance(element, int) for element in value):
            raise TypeError("sponsors_id must be a list of ints")
        self._sponsors_id = value

    @property
    def cosponsors(self) -> List[str]:
        return self._cosponsors
    @cosponsors.setter
    def cosponsors(self, value: List[str]) -> None:
        if type(value) != list:
            raise TypeError("cosponsors must be a list of strs")
        if not all(isinstance(element, str) for element in value):
            raise TypeError("cosponsors must be a list of strs")
        self._cosponsors = value

    @property
    def cosponsors_id(self) -> List[int]:
        return self._cosponsors_id
    @cosponsors_id.setter
    def cosponsors_id(self, value: List[int]) -> None:
        if type(value) != list:
            raise TypeError("cosponsors_id must be a list of ints")
        if not all(isinstance(element, int) for element in value):
            raise TypeError("cosponsors_id must be a list of ints")
        self._cosponsors_id = value

    @property
    def bill_text(self) -> str:
        return self._bill_text
    @bill_text.setter
    def bill_text(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("bill_text must be a str")
        self._bill_text = value

    @property
    def bill_description(self) -> str:
        return self._bill_description
    @bill_description.setter
    def bill_description(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("bill_description must be a str")
        self._bill_description = value

    @property
    def bill_summary(self) -> str:
        return self._bill_summary
    @bill_summary.setter
    def bill_summary(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("bill_summary must be a str")
        self._bill_summary = value

    @property
    def actions(self) -> List[dict]:
        return self._actions
    @actions.setter
    def actions(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("actions must be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("actions must a list of dicts")
            if element.get('date') is None or type(element.get('date')) != str:
                raise ValueError("actions data must have valid 'date' information as a str")
            if not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', element.get('date')):
                raise ValueError("Improper date formating in actions['date']. Required format: YYYY-MM-DD")
            if element.get('action_by') is None or type(element.get('action_by')) != str:
                raise ValueError("actions data must have valid 'action_by' information as a str")
            if element.get('description') is None or type(element.get('description')) != str:
                raise ValueError("actions data must have valid 'description' information as a str")
        self._actions = value

    @property
    def votes(self) -> List[dict]:
        return self._votes
    @votes.setter
    def votes(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("votes must be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("votes must a list of dicts")
            if element.get('date') is None or type(element.get('date')) != str:
                raise ValueError("votes data must have valid 'date' information as a str")
            if not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', element.get('date')):
                raise ValueError("Improper date formating in votes['date']. Required format: YYYY-MM-DD")
            if element.get('description') is None or type(element.get('description')) != str:
                raise ValueError("votes data must have valid 'description' information as a str")
            if element.get('yea') is None or type(element.get('yea')) != int:
                raise ValueError("votes data must have valid 'yea' information as an int")
            if element.get('nay') is None or type(element.get('nay')) != int:
                raise ValueError("votes data must have valid 'nay' information as an int")
            if element.get('nv') is None or type(element.get('nv')) != int:
                raise ValueError("votes data must have valid 'nv' information as an int")
            if element.get('absent') is None or type(element.get('absent')) != int:
                raise ValueError("votes data must have valid 'absent' information as an int")
            if element.get('total') is None or type(element.get('total')) != int:
                raise ValueError("votes data must have valid 'total' information as an int")
            if element.get('total') != (element.get('yea') + element.get('nay') + element.get('nv') + \
                element.get('absent')):
                raise ValueError("votes data does not add up to total") 
            if element.get('passed') is None or type(element.get('passed')) != int:
                raise ValueError("votes data must have valid 'passed' information as an int")
            if element.get('passed') != 0 and element.get('passed') != 1:
                raise ValueError("votes passed data must be a 0 (not passed) or 1 (passed)")
            if element.get('chamber') is None or type(element.get('chamber')) != str:
                raise ValueError("votes data must have valid 'chamber' information as an str")
            if element.get('votes') is None or type(element.get('votes')) != list:
                raise ValueError("votes data must have valid 'votes' information as a list of dicts")
            for votes_data in element.get('votes'):
                if type(votes_data) != dict:
                    raise ValueError("votes data must have valid 'votes' information as a list of dicts")
                if votes_data.get('goverlytics_id') is None or type(votes_data.get('goverlytics_id')) != int:
                    raise ValueError("votes in votes data must have valid 'goverlytics_id' information as an int")
                if votes_data.get('legislator') is None or type(votes_data.get('legislator')) != str:
                    raise ValueError("votes in votes data must have valid 'legislator' information as a str")
                if votes_data.get('vote_text') is None or type(votes_data.get('vote_text')) != str:
                    raise ValueError("votes in votes data must have valid 'vote_text' information as a str")
        self._votes = value
        
    @property
    def source_topic(self) -> str:
        return self._source_topic
    @source_topic.setter
    def source_topic(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("source_topic must be a str")
        self._source_topic = value

    @property
    def topic(self) -> str:
        return self._topic
    @topic.setter
    def topic(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("topic must be a str")
        self._topic = value

    @property
    def country_id(self) -> str:
        return self._country_id
    @country_id.setter
    def country_id(self, id: int) -> None:
        if type(id) != int:
            raise TypeError("country_id must be a int")
        self._country_id = id

    @property
    def country(self) -> str:
        return self._country
    @country.setter
    def country(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("country must be a str")
        self._country = value

@dataclass
class USLegislationRow(LegislationRow):
    """
    Data structure for housing data about each piece of legislation.
    """
    state_id: int
    state: str

    def __init__(self):
        super().__init__()
        self._state_id = 0
        self._state = ''

    @property
    def state_id(self) -> int:
        return self._state_id
    @state_id.setter
    def state_id(self, id: int) -> None:
        if type(id) != int:
            raise TypeError("state_id must be an int")
        self._state_id = id

    @property
    def state(self) -> str:
        return self._state
    @state.setter
    def state(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("state must be a str")
        if len(value) != 2:
            raise ValueError("Improper state formatting. Required format: XX (ex. AL, TX")
        self._state = value

@dataclass
class CALegislationRow(LegislationRow):
    """
    Data structure for housing data about each piece of legislation.
    """
    province_territory_id: int = 0
    province_territory: str = ''
    region: str = ''

    def __init__(self):
        super().__init__()
        self._province_territory_id = 0
        self._province_territory = ''
        self._region = ''

    @property
    def province_territory_id(self) -> int:
        return self._province_territory_id
    @province_territory_id.setter
    def province_territory_id(self, id: int) -> None:
        if type(id) != int:
            raise TypeError("province_territory_id must be an int")
        self._province_territory_id = id

    @property
    def province_territory(self) -> str:
        return self._province_territory
    @province_territory.setter
    def province_territory(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("province_territory must be a str")
        if len(value) != 2:
            raise ValueError("Improper province_territory formatting. Required format: XX (ex. BC, NU")
        self._province_territory = value

    @property
    def region(self) -> str:
        return self._region
    @region.setter
    def region(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("region must be a str")
        self._region = value

@dataclass
class CAFedLegislationRow(CALegislationRow):
    """
    Data structure for housing data about each piece of legislation.
    """
    sponsor_affiliation: str
    sponsor_gender: str
    pm_name_full: str
    pm_party: str
    pm_party_id: int
    statute_year: int
    statute_chapter: int
    publications: List[str]
    last_major_event: dict

    def __init__(self):
        super().__init__()
        self._sponsor_affiliation = ''
        self._sponsor_gender= ''
        self._pm_name_full = ''
        self._pm_party = ''
        self._pm_party_id = 0
        self._statute_year = 0
        self._statute_chapter = 0
        self._publications = []
        self._last_major_event = {}

    @property
    def sponsor_affiliation(self) -> str:
        return self._sponsor_affiliation
    @sponsor_affiliation.setter
    def sponsor_affiliation(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("sponsor_affiliation must be a str")
        self._sponsor_affiliation = value

    @property
    def sponsor_gender(self) -> str:
        return self._sponsor_gender
    @sponsor_gender.setter
    def sponsor_gender(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("sponsor_gender must be a str")
        if value != 'M' and value != 'F':
            raise ValueError("Improper gender formatting. Required format: M or F. This may change in the future to reflect all parts of the spectrum.")
        self._sponsor_gender = value

    @property
    def pm_name_full(self) -> str:
        return self._pm_name_full
    @pm_name_full.setter
    def pm_name_full(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("pm_name_full must be a str")
        name_split = value.split(' ')
        if len(name_split) <= 1:
            raise ValueError("Improper name formatting in pm_name_full. Required format: Full name (ex. Justin Trudeau")
        self._pm_name_full = value
        
    @property
    def pm_party(self) -> str:
        return self._pm_party
    @pm_party.setter
    def pm_party(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("pm_party must be a str")
        self._pm_party = value

    @property
    def pm_party_id(self) -> int:
        return self._pm_party_id
    @pm_party_id.setter
    def pm_party_id(self, value: int) -> None:
        if type(value) != int:
            raise TypeError("pm_party_id must be an int")
        self._pm_party_id = value

    @property
    def statute_year(self) -> int:
        return self._statute_year
    @statute_year.setter
    def statute_year(self, value: int) -> None:
        if type(value) != int:
            raise TypeError("statute_year must be an int")
        if value < 999:
            raise ValueError("Improper year formatting in statute_year. Required format: YYYY")
        self._statute_year = value

    @property
    def statute_chapter(self) -> int:
        return self._statute_chapter
    @statute_chapter.setter
    def statute_chapter(self, value: int) -> None:
        if type(value) != int:
            raise TypeError("statute_chapter must be an int")
        self._statute_chapter = value

    @property
    def publications(self) -> List[str]:
        return self._publications
    @publications.setter
    def publications(self, value: List[str]) -> None:
        if type(value) != list:
            raise TypeError("publications must be a list of strs")
        if not all(isinstance(element, str) for element in value):
            raise TypeError("publications must be a list of strs")
        self._publications = value

    @property
    def last_major_event(self) -> dict:
        return self._last_major_event
    @last_major_event.setter
    def last_major_event(self, value: dict) -> None:
        if type(value) != list:
            raise TypeError("last_major_event must be a dict")
        if value.get('date') is None or type(value.get('date')) != str:
            raise ValueError("last_major_event data must have valid 'date' information as a str")
        if value.get('status') is None or type(value.get('status')) != str:
            raise ValueError("last_major_event data must have valid 'status' information as a str")
        if value.get('chamber') is None or type(value.get('chamber')) != str:
            raise ValueError("last_major_event data must have valid 'chamber' information as a str")
        if value.get('committee') is None or type(value.get('committee')) != str:
            raise ValueError("last_major_event data must have valid 'committee' information as a str")
        if value.get('meeting_number') == None or type(value.get('meeting_number')) != int:
            raise ValueError("last_major_event data must have valid 'meeting_number' information as a int")
        self._last_major_event = value


#########################################################
#       LEGISLATOR ROWS                                 #
#########################################################
@dataclass
class LegislatorRow:
    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    source_id: str 
    most_recent_term_id: str 
    source_url: str 
    name_full: str 
    name_last: str 
    name_first: str 
    name_middle: str 
    name_suffix: str 
    country_id: int
    country: str 
    party_id: int
    party: str 
    role: str 
    years_active: List[int]
    committees: List[dict]
    phone_numbers: List[dict]
    addresses: List[dict]
    email: str 
    birthday: datetime 
    seniority: int 
    occupation: List[str]
    education: List[dict]
    military_experience: str 

    def __init__(self):
        self._source_id = ''
        self._most_recent_term_id = ''
        self._source_url = ''
        self._name_full = ''
        self._name_last = ''
        self._name_first = ''
        self._name_middle = ''
        self._name_suffix = ''
        self._country_id = None
        self._country = ''
        self._party_id = None
        self._party = ''
        self._role = ''
        self._years_active = []
        self._committees = []
        self._phone_numbers = []
        self._addresses = []
        self._email = ''
        self._birthday = None
        self._seniority = 0
        self._occupation = []
        self._education = []
        self._military_experience = ''

    @property
    def source_id(self) -> str:
        return self._source_id
    @source_id.setter
    def source_id(self, id: str) -> None:
        if type(id) != str:
            raise TypeError("source_id must be a str")
        self._source_id = id

    @property
    def most_recent_term_id(self) -> str:
        return self._most_recent_term_id
    @most_recent_term_id.setter
    def most_recent_term_id(self, id: str) -> None:
        if type(id) != str:
            raise TypeError("most_recent_term_id must be a str")
        self._most_recent_term_id = id

    @property
    def source_url(self) -> str:
        return self._source_url
    @source_url.setter
    def source_url(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("source_url must be a str")
        self._source_url = value

    @property
    def name_full(self) -> str:
        return self._name_full
    @name_full.setter
    def name_full(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("name_full must be a str")
        self._name_full = value

    @property
    def name_last(self) -> str:
        return self._name_last
    @name_last.setter
    def name_last(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("name_last must be a str")
        self._name_last = value

    @property
    def name_first(self) -> str:
        return self._name_first
    @name_first.setter
    def name_first(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("name_first must be a str")
        self._name_first = value

    @property
    def name_middle(self) -> str:
        return self._name_middle
    @name_middle.setter
    def name_middle(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("name_middle must be a str")
        self._name_middle = value
        
    @property
    def name_suffix(self) -> str:
        return self._name_suffix
    @name_suffix.setter
    def name_suffix(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("name_suffix must be a str")
        self._name_suffix = value

    @property
    def country_id(self) -> int:
        return self._country_id
    @country_id.setter
    def country_id(self, id: int) -> None:
        if type(id) != int:
            raise TypeError("country_id must be an int")
        self._country_id = id

    @property
    def country(self) -> str:
        return self._country
    @country.setter
    def country(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("country must be a str")
        self._country = value

    @property
    def party_id(self) -> int:
        return self._party_id
    @party_id.setter
    def party_id(self, id: int) -> None:
        if type(id) != int:
            raise TypeError("party_id must be an int")
        self._party_id = id

    @property
    def party(self) -> str:
        return self._party
    @party.setter
    def party(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("party must be a str")
        self._party = value

    @property
    def role(self) -> str:
        return self._role
    @role.setter
    def role(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("role must be a str")
        self._role = value

    @property
    def years_active(self) -> List[int]:
        return self._years_active
    @years_active.setter
    def years_active(self, value: List[int]) -> None:
        if type(value) != list:
            raise TypeError("years_active must be a list of ints")
        for year in value:
            if type(year) != int:
                raise TypeError("years_active must be a list of ints")
            if year < 999:
                raise ValueError("Improper year formatting in years_active. Required format: YYYY")
        self._years_active = value

    @property
    def committees(self) -> List[dict]:
        return self._committees
    @committees.setter
    def committees(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("committees must be a list of dicts")
        for committee in value:
            if type(committee) != dict:
                raise TypeError("committees must be a list of dicts")
            if committee.get('role') is None or type(committee.get('role')) != str:
                raise ValueError("committees data must have valid 'role' information as a str")
            if committee.get('committee') is None or type(committee.get('committee')) != str:
                raise ValueError("committees data must have valid 'committee' information as a str")
        self._committees = value

    @property
    def phone_numbers(self) -> List[dict]:
        return self._phone_numbers
    @phone_numbers.setter
    def phone_numbers(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("phone_numbers must be a list of dicts")
        for number in value:
            if type(number) != dict:
                raise TypeError("phone_numbers must be a list of dicts")
            if number.get('office') is None or type(number.get('office')) != str:
                raise ValueError("phone_numbers data must have valid 'office' information as a str")
            if number.get('number') is None or type(number.get('number')) != str:
                raise ValueError("phone_numbers data must have valid 'number' information as a str")
            if not re.match(r'[0-9]{3}-[0-9]{3}-[0-9]{4}', number.get('number')):
                raise ValueError("Improper number formatting in phone_numbers. Required format: ###-###-####")
        self._phone_numbers = value

    @property
    def addresses(self) -> List[dict]:
        return self._addresses
    @addresses.setter
    def addresses(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("addresses must be a list of dicts")
        for address in value:
            if type(address) != dict:
                raise TypeError("addresses must be a list of dicts")
            if address.get('location') is None or type(address.get('location')) != str:
                raise ValueError("addresses data must have valid 'location' information as a str")
            if address.get('address') is None or type(address.get('address')) != str:
                raise ValueError("addresses data must have valid 'address' information as a str")
        self._addresses = value

    @property
    def email(self) -> str:
        return self._email
    @email.setter
    def email(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("email must be a str")
        email_regex = r'\b[A-Za-z0-9._%\'!+-]+@[A-Za-z0-9.-]+.[A-Z|a-z]{2,}\b'
        if not re.match(email_regex, value):
            raise ValueError("Improper email formatting in email. Required format: ExampleEmail@domain.com" +
                "\nThere may be a problem with the email validator so please check rows.py for more details.")

    @property
    def birthday(self) -> datetime:
        return self._birthday
    @birthday.setter
    def birthday(self, value: datetime) -> None:
        if type(value) != datetime:
            raise TypeError("birthday must be a datetime")
        self._birthday = value

    @property
    def seniority(self) -> int:
        return self._seniority
    @seniority.setter
    def seniority(self, value: int) -> None:
        if type(value) != int:
            raise TypeError("seniority must be an int")
        self._seniority = value

    @property
    def occupation(self) -> List[str]:
        return self._occupation
    @occupation.setter
    def occupation(self, value: List[str]) -> None:
        if type(value) != list:
            raise TypeError("occupation must be a list of strs")
        if not all(isinstance(element, str) for element in value):
            raise TypeError("occupation must be a list of strs")
        self._occupation = value

    @property
    def education(self) -> List[dict]:
        return self._education
    @education.setter
    def education(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("education must be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("education must be a list of dicts")
            if element.get('level') is None or type(element.get('level')) != str:
                raise ValueError("education data must have valid 'level' information as a str")
            if element.get('field') is None or type(element.get('field')) != str:
                raise ValueError("education data must have valid 'field' information as a str")
            if element.get('school') is None or type(element.get('school')) != str:
                raise ValueError("education data must have valid 'school' information as a str")
        self._education = value

    @property
    def military_experience(self) -> str:
        return self._military_experience
    @military_experience.setter
    def military_experience(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("military_experience must be a str")
        self._military_experience = value

@dataclass
class USLegislatorRow(LegislatorRow):
    """
    Data structure for housing data about each piece of legislator.
    """
    state_id: int
    state: str
    areas_served: List[str]
    district: str

    def __init__(self):
        super().__init__()
        self._state_id = None
        self._state = ''
        self._areas_served = []
        self._district = ''

    @property
    def state_id(self) -> int:
        return self._state_id
    @state_id.setter
    def state_id(self, id: int) -> None:
        if type(id) != int:
            raise TypeError("state_id must be an int")
        self._state_id = id

    @property
    def state(self) -> str:
        return self._state
    @state.setter
    def state(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("state must be a str")
        self._state = value

    @property
    def areas_served(self) -> List[str]:
        return self._areas_served
    @areas_served.setter
    def areas_served(self, value: List[str]) -> None:
        if type(value) != list:
            raise TypeError("areas_served must be a list of str")
        if not all(isinstance(element, str) for element in value):
            raise TypeError("areas_served must be a list of strs")
        self._areas_served = value

    @property
    def district(self) -> str:
        return self._district
    @district.setter
    def district(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("district must be a str")
        self._district = value

@dataclass
class CALegislatorRow(LegislatorRow):
    """
    Data structure for housing data about each piece of legislator.
    """

    province_territory_id: int
    province_territory: str
    riding: str
    region: str

    def __init__(self):
        super().__init__()
        self._province_territory_id = None
        self._province_territory = ''
        self._riding = ''
        self._region = ''

    @property
    def province_territory_id(self) -> int:
        return self._province_territory_id
    @province_territory_id.setter
    def province_territory_id(self, id: int) -> None:
        if type(id) != int:
            raise TypeError("province_territory_id must be an int")
        self._province_territory_id = id

    @property
    def province_territory(self) -> str:
        return self._province_territory
    @province_territory.setter
    def province_territory(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("province_territory must be a str")
        self._province_territory = value

    @property
    def riding(self) -> str:
        return self._riding
    @riding.setter
    def riding(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("riding must be a str")
        self._riding = value

    @property
    def region(self) -> str:
        return self._region
    @region.setter
    def region(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("region must be a str")
        self._region = value

@dataclass
class CAFedLegislatorRow(CALegislatorRow):
    """
    Data structure for housing data about each piece of legislator.
    """
    offices_roles_as_mp: List[str]
    parl_assoc_interparl_groups: List[dict]

    def __init__(self):
        super().__init__()
        self._offices_roles_as_mp = []
        self._parl_assoc_interparl_groups = []

    @property
    def offices_roles_as_mp(self) -> List[str]:
        return self._offices_roles_as_mp
    @offices_roles_as_mp.setter
    def offices_roles_as_mp(self, value: List[str]) -> None:
        if type(value) != list:
            raise TypeError("offices_roles_as_mp must be a list of str")
        if not all(isinstance(element, str) for element in value):
            raise TypeError("offices_roles_as_mp must be a list of str")
        self._offices_roles_as_mp = value

    @property
    def parl_assoc_interparl_groups(self) -> List[dict]:
        return self._parl_assoc_interparl_groups
    @parl_assoc_interparl_groups.setter
    def parl_assoc_interparl_groups(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("parl_assoc_interparl_groups must be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("parl_assoc_interparl_groups must be a list of dicts")
            if element.get('role') is None or type(element.get('role')) != str:
                raise ValueError("parl_assoc_interparl_groups data must have valid 'role' information as a str")
            if element.get('title') is None or type(element.get('title')) != str:
                raise ValueError("parl_assoc_interparl_groups data must have valid 'title' information as a str")
            if element.get('organization') is None or type(element.get('organization')) != str:
                raise ValueError("parl_assoc_interparl_groups data must have valid 'organization' information as a str")
        self._parl_assoc_interparl_groups = value