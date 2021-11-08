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
    date_introduced: str
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
        self._date_introduced = ''
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
        if id is not None and type(id) != int:
            raise TypeError("principal_sponsor_id must be a int")
        self._principal_sponsor_id = id

    @property
    def principal_sponsor(self) -> str:
        return self._principal_sponsor
    @principal_sponsor.setter
    def principal_sponsor(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("principal_sponsor must be a str")
        if len(value) == 0:
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
                    raise TypeError("votes data must have valid 'votes' information as a list of dicts")
                if votes_data.get('goverlytics_id') is not None and type(votes_data.get('goverlytics_id')) != int:
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
        if id is not None and type(id) != int:
            raise TypeError("province_territory_id must be an int")
        self._province_territory_id = id

    @property
    def province_territory(self) -> str:
        return self._province_territory
    @province_territory.setter
    def province_territory(self, value: str) -> None:
        if value is not None and type(value) != str:
            raise TypeError("province_territory must be a str")
        if value is not None and len(value) != 2:
            raise ValueError("Improper province_territory formatting. Required format: XX (ex. BC, NU")
        self._province_territory = value

    @property
    def region(self) -> str:
        return self._region
    @region.setter
    def region(self, value: str) -> None:
        if value is not None and type(value) != str:
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
        if value != 'M' and value != 'F' and value != 'O':
            raise ValueError("Improper gender formatting. Required format: M or F or O. This may change in the future to reflect all parts of the spectrum.")
        self._sponsor_gender = value

    @property
    def pm_name_full(self) -> str:
        return self._pm_name_full
    @pm_name_full.setter
    def pm_name_full(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("pm_name_full must be a str")
        name_split = value.split()
        if len(name_split) <= 1:
            raise ValueError("Improper name formatting in pm_name_full. Required format: Full name (ex. Justin Trudeau)")
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
        if value < 999 and value != 0:
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
        if type(value) != dict:
            raise TypeError("last_major_event must be a dict")
        if value.get('date') is None or type(value.get('date')) != str:
            raise ValueError("last_major_event data must have valid 'date' information as a str")
        if not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value.get('date')):
            raise ValueError("Improper date formating in last_major_event['date]. Required format: YYYY-MM-DD")
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
    gender: str
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
    wiki_url: str

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
        self._gender = ''
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
        self._wiki_url = ''

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
    def gender(self) -> str:
        return self._gender
    @gender.setter
    def gender(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("gender must be a str")
        accepted_values = ['M', 'F', 'O']
        if value not in accepted_values:
            raise ValueError("gender must be one of 'M' (Male), 'F' (Female), 'O' (Other)")
        self._gender = value

    @property
    def years_active(self) -> List[int]:
        return self._years_active
    @years_active.setter
    def years_active(self, value: List[int]) -> None:
        if type(value) != list:
            raise TypeError("years_active must be a list of ints")
        if not all(isinstance(x, int) for x in value):
            raise TypeError("years_active must be a list of ints")
        if not all(x > 999 for x in value):
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
            if not (re.match(r'[0-9]{3}-[0-9]{3}-[0-9]{4}', number.get('number')) or 
                    re.match(r'[0-9]{1,}-[0-9]{3}-[0-9]{3}-[0-9]{4}', number.get('number'))):
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
        email_regex = r'[A-Za-z0-9._%\'!+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}'
        if not re.match(email_regex, value):
            raise ValueError("Improper email formatting in email. Required format: ExampleEmail@domain.com" +
                f"\nEmail passed in was {value}" +
                "\nThere may be a problem with the email validator so please check rows.py for more details.")
        self._email = value

    @property
    def birthday(self) -> datetime:
        return self._birthday
    @birthday.setter
    def birthday(self, value: datetime) -> None:
        if value is not None and not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', str(value)):
            raise ValueError("Improper birthday formatting. Required format: ####-##-##")
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

    @property
    def wiki_url(self) -> str:
        return self._wiki_url
    @wiki_url.setter
    def wiki_url(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("wiki_url must be a str")
        self._wiki_url = value


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
class LegislatorSponsorTopicRow:
    """ 
    Data structure for housing the number of bills sponsored by a Legislator
    and that bills respective CAP topic
    """
    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value
    
    name_full: str 
    name_last: str 
    name_first: str 
    name_middle: str 
    name_suffix: str 
    party: str
    agriculture: int
    civil_rights: int
    defense: int
    domestic_commerce: int
    education: int
    energy: int
    environment: int
    foreign_trade: int
    government_operations: int
    health: int
    immigration: int
    international_affairs: int
    labor: int
    law_and_crime: int
    macroeconomics: int
    social_welfare: int
    technology: int
    transportation: int

    def __init__(self):
        super().__init__()
        self._name_full = ''
        self._name_last = ''
        self._name_first = ''
        self._name_middle = ''
        self._name_suffix = ''
        self._party = ''
        self._agriculture = None
        self._civil_rights = None
        self._defense = None
        self._domestic_commerce = None
        self._education = None
        self._energy = None
        self._environment = None
        self._foreign_trade = None
        self._government_operations = None
        self._health = None
        self._immigration = None
        self._international_affairs = None
        self._labor = None
        self._law_and_crime = None
        self._macroeconomics = None
        self._social_welfare = None
        self._technology = None
        self._transportation = None

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
    def party(self) -> str:
        return self._party
    @party.setter
    def party(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("party must be a str")
        self._party = value

    @property
    def agriculture(self) -> int:
        return self._agriculture
    @agriculture.setter
    def agriculture(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("agriculture count must be an integer")
        self._agriculture = count

    @property
    def civil_rights(self) -> int:
        return self._civil_rights
    @civil_rights.setter
    def civil_rights(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("civil_rights count must be an integer")
        self._civil_rights = count
    
    @property
    def defense(self) -> int:
        return self._defense
    @defense.setter
    def defense(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("defense count must be an integer")
        self._defense = count

    @property
    def domestic_commerce(self) -> int:
        return self._domestic_commerce
    @domestic_commerce.setter
    def domestic_commerce(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("domestic_commerce count must be an integer")
        self._domestic_commerce = count

    @property
    def education(self) -> int:
        return self._education
    @education.setter
    def education(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("education count must be an integer")
        self._education = count

    @property
    def energy(self) -> int:
        return self._energy
    @energy.setter
    def energy(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("energy count must be an integer")
        self._energy = count

    @property
    def environment(self) -> int:
        return self._environment
    @environment.setter
    def environment(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("environment count must be an integer")
        self._environment = count

    @property
    def foreign_trade(self) -> int:
        return self._foreign_trade
    @foreign_trade.setter
    def foreign_trade(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("foreign_trade count must be an integer")
        self._foreign_trade = count

    @property
    def government_operations(self) -> int:
        return self._government_operations
    @government_operations.setter
    def government_operations(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("government_operations count must be an integer")
        self._government_operations = count

    @property
    def health(self) -> int:
        return self._health
    @health.setter
    def health(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("health count must be an integer")
        self._health = count

    @property
    def immigration(self) -> int:
        return self._immigration
    @immigration.setter
    def immigration(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("immigration count must be an integer")
        self._immigration = count

    @property
    def international_affairs(self) -> int:
        return self._international_affairs
    @international_affairs.setter
    def international_affairs(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("international_affairs count must be an integer")
        self._international_affairs = count

    @property
    def labor(self) -> int:
        return self._labor
    @labor.setter
    def labor(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("labor count must be an integer")
        self._labor = count

    @property
    def law_and_crime(self) -> int:
        return self._law_and_crime
    @law_and_crime.setter
    def law_and_crime(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("law_and_crime count must be an integer")
        self._law_and_crime = count

    @property
    def macroeconomics(self) -> int:
        return self._macroeconomics
    @macroeconomics.setter
    def macroeconomics(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("macroeconomics count must be an integer")
        self._macroeconomics = count

    @property
    def social_welfare(self) -> int:
        return self._social_welfare
    @social_welfare.setter
    def social_welfare(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("social_welfare count must be an integer")
        self._social_welfare = count

    @property
    def technology(self) -> int:
        return self._technology
    @technology.setter
    def technology(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("technology count must be an integer")
        self._technology = count

    @property
    def transportation(self) -> int:
        return self._transportation
    @transportation.setter
    def transportation(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("transportation count must be an integer")
        self._transportation = count


@dataclass
class CAFedLegislatorRow(CALegislatorRow):
    """
    Data structure for housing data about each piece of legislator.
    """
    offices_roles_as_mp: List[str]
    parl_assoc_interparl_groups: List[dict]
    years_of_service: str

    def __init__(self):
        super().__init__()
        self._offices_roles_as_mp = []
        self._parl_assoc_interparl_groups = []
        self._years_of_service = ''

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

    @property
    def years_of_service(self) -> str:
        return self._years_of_service
    @years_of_service.setter
    def years_of_service(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("years_of_service must be a str")
        self._years_of_service = value

@dataclass
class LegislatorSponsorTopicRow:
    """ 
    Data structure for housing the number of bills sponsored by a Legislator
    and that bills respective CAP topic
    """
    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value
            
    goverlytics_id: int
    name_full: str 
    name_last: str 
    name_first: str 
    name_middle: str 
    name_suffix: str 
    party: str
    country: str
    agriculture: int
    civil_rights: int
    defense: int
    domestic_commerce: int
    edu_cation: int
    energy: int
    environment: int
    foreign_trade: int
    government_operations: int
    health: int
    immigration: int
    international_affairs: int
    labor: int
    law_and_crime: int
    macroeconomics: int
    social_welfare: int
    technology: int
    transportation: int

    def __init__(self):
        super().__init__()
        self._goverlytics_id = ''
        self._name_full = ''
        self._name_last = ''
        self._name_first = ''
        self._name_middle = ''
        self._name_suffix = ''
        self._party = ''
        self._country = ''
        self._agriculture = None
        self._civil_rights = None
        self._defense = None
        self._domestic_commerce = None
        self._edu_cation = None
        self._energy = None
        self._environment = None
        self._foreign_trade = None
        self._government_operations = None
        self._health = None
        self._immigration = None
        self._international_affairs = None
        self._labor = None
        self._law_and_crime = None
        self._macroeconomics = None
        self._social_welfare = None
        self._technology = None
        self._transportation = None

    @property
    def goverlytics_id(self) -> str:
        return self._goverlytics_id
    @goverlytics_id.setter
    def goverlytics_id(self, id: str) -> None:
        if not isinstance(id, int):
            raise TypeError("goverlytics_id must be an str")
        self._goverlytics_id = id

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
    def country(self) -> str:
        return self._country
    @country.setter
    def country(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("country must be a str")
        self._country = value
    
    @property
    def party(self) -> str:
        return self._party
    @party.setter
    def party(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("party must be a str")
        self._party = value

    @property
    def agriculture(self) -> int:
        return self._agriculture
    @agriculture.setter
    def agriculture(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("agriculture count must be an integer")
        self._agriculture = count

    @property
    def civil_rights(self) -> int:
        return self._civil_rights
    @civil_rights.setter
    def civil_rights(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("civil_rights count must be an integer")
        self._civil_rights = count
    
    @property
    def defense(self) -> int:
        return self._defense
    @defense.setter
    def defense(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("defense count must be an integer")
        self._defense = count

    @property
    def domestic_commerce(self) -> int:
        return self._domestic_commerce
    @domestic_commerce.setter
    def domestic_commerce(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("domestic_commerce count must be an integer")
        self._domestic_commerce = count

    @property
    def edu_cation(self) -> int:
        return self._edu_cation
    @edu_cation.setter
    def edu_cation(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("edu_cation count must be an integer")
        self._edu_cation = count

    @property
    def energy(self) -> int:
        return self._energy
    @energy.setter
    def energy(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("energy count must be an integer")
        self._energy = count

    @property
    def environment(self) -> int:
        return self._environment
    @environment.setter
    def environment(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("environment count must be an integer")
        self._environment = count

    @property
    def foreign_trade(self) -> int:
        return self._foreign_trade
    @foreign_trade.setter
    def foreign_trade(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("foreign_trade count must be an integer")
        self._foreign_trade = count

    @property
    def government_operations(self) -> int:
        return self._government_operations
    @government_operations.setter
    def government_operations(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("government_operations count must be an integer")
        self._government_operations = count

    @property
    def health(self) -> int:
        return self._health
    @health.setter
    def health(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("health count must be an integer")
        self._health = count

    @property
    def immigration(self) -> int:
        return self._immigration
    @immigration.setter
    def immigration(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("immigration count must be an integer")
        self._immigration = count

    @property
    def international_affairs(self) -> int:
        return self._international_affairs
    @international_affairs.setter
    def international_affairs(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("international_affairs count must be an integer")
        self._international_affairs = count

    @property
    def labor(self) -> int:
        return self._labor
    @labor.setter
    def labor(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("labor count must be an integer")
        self._labor = count

    @property
    def law_and_crime(self) -> int:
        return self._law_and_crime
    @law_and_crime.setter
    def law_and_crime(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("law_and_crime count must be an integer")
        self._law_and_crime = count

    @property
    def macroeconomics(self) -> int:
        return self._macroeconomics
    @macroeconomics.setter
    def macroeconomics(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("macroeconomics count must be an integer")
        self._macroeconomics = count

    @property
    def social_welfare(self) -> int:
        return self._social_welfare
    @social_welfare.setter
    def social_welfare(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("social_welfare count must be an integer")
        self._social_welfare = count

    @property
    def technology(self) -> int:
        return self._technology
    @technology.setter
    def technology(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("technology count must be an integer")
        self._technology = count

    @property
    def transportation(self) -> int:
        return self._transportation
    @transportation.setter
    def transportation(self, count: int) -> None:
        if type(count) != int:
            raise TypeError("transportation count must be an integer")
        self._transportation = count

#########################################################
#       ELECTION ROWS                                   #
#########################################################

@dataclass
class ElectionRow:
    """
    Data structure for housing election data
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    election_name: str
    election_date: str
    official_votes_record_url: str
    description: str
    is_by_election: bool

    def __init__(self):
        self._election_name = ''
        self._election_date = ''
        self._official_votes_record_url = ''
        self._description = ''
        self._is_by_election = None

    @property
    def election_name(self) -> str:
        return self._election_name
    @election_name.setter
    def election_name(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("election_name must be a str")
        self._election_name = value

    @property
    def election_date(self) -> str:
        return self._election_date
    @election_date.setter
    def election_date(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("election_date must be a str")
        if value and not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
            raise ValueError("Improper date formating in election_date. Required format: YYYY-MM-DD")
        self._election_date = value

    @property
    def official_votes_record_url(self) -> str:
        return self._official_votes_record_url
    @official_votes_record_url.setter
    def official_votes_record_url(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("official_votes_record_url must be a str")
        self._official_votes_record_url = value

    @property
    def description(self) -> str:
        return self._description
    @description.setter
    def description(self, value: str) -> None:
        if type(value) != str:
            raise TypeError("description must be a str")
        self._description = value

    @property
    def is_by_election(self) -> bool:
        return self._is_by_election
    @is_by_election.setter
    def is_by_election(self, value: bool) -> None:
        if type(value) != bool:
            raise TypeError("is_by_election must be a bool")
        self._is_by_election = value

@dataclass
class ElectoralDistrictsRow:
    """
    Data structure for housing data for electoral districts
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    province_territory_id: int
    population: int
    census_year: int
    prev_district_names: List[str]
    district_name: str
    region: str
    is_active: bool
    start_date: str

    def __init__(self):
        self._province_territory_id = None
        self._population = None
        self._census_year = None
        self._prev_district_names = []
        self._district_name = ''
        self._region = ''
        self._is_active = None
        self._start_date = ''

    @property
    def province_territory_id(self) -> int:
        return self._province_territory_id
    @province_territory_id.setter
    def province_territory_id(self, id: int) -> None:
        if not isinstance(id, int):
            raise TypeError("province_territory_id must be an int")
        self._province_territory_id = id

    @property
    def population(self) -> int:
        return self._population
    @population.setter
    def population(self, id: int) -> None:
        if (not isinstance(id, int)) and id != None:
            raise TypeError("population must be an int")
        self._population = id

    @property
    def census_year(self) -> int:
        return self._census_year
    @census_year.setter
    def census_year(self, id: int) -> None:
        if not isinstance(id, int):
            raise TypeError("census_year must be an int")
        self._census_year = id

    @property
    def prev_district_names(self) -> list:
        return self._prev_district_names
    @prev_district_names.setter
    def prev_district_names(self, value: list) -> None:
        if not isinstance(value, list):
            raise TypeError("prev_district_names must be a list of str")
        if not all(isinstance(element, str) for element in value):
            raise TypeError("prev_district_names must be a list of strs")
        self._prev_district_names = value

    @property
    def district_name(self) -> str:
        return self._district_name
    @district_name.setter
    def district_name(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("district_name must be a str")
        self._district_name = value

    @property
    def region(self) -> str:
        return self._region
    @region.setter
    def region(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("region must be a str")
        self._region = value

    @property
    def is_active(self) -> str:
        return self._is_active
    @is_active.setter
    def is_active(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("is_active must be a bool")
        self._is_active = value

    @property
    def start_date(self) -> str:
        return self._start_date
    @start_date.setter
    def start_date(self, value: str) -> None:
        if value and not isinstance(value, str):
            raise TypeError("start_date must be a str")
        if value and not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
            raise ValueError("Improper date formating in start_date. Required format: YYYY-MM-DD")
        self._start_date = value


@dataclass
class ElectorsRow:
    """
    Data structure for housing data for electors
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    province_territory_id: int
    population: int
    electors: int
    election_id: int

    def __init__(self):
        self._province_territory_id = None
        self._population = None
        self._electors = None
        self._election_id = None

    @property
    def province_territory_id(self) -> int:
        return self._province_territory_id

    @province_territory_id.setter
    def province_territory_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("province_territory_id must be an int")
        self._province_territory_id = value

    @property
    def population(self) -> int:
        return self._population

    @population.setter
    def population(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("population must be an int")
        self._population = value

    @property
    def electors(self) -> int:
        return self._electors

    @electors.setter
    def electors(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("electors must be an int")
        self._electors = value

    @property
    def election_id(self) -> int:
        return self._election_id

    @election_id.setter
    def election_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("election_id must be aan int")
        self._election_id = value

@dataclass
class CandidatesRow:
    """
    Data structure for housing data for candidates since 1867
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    goverlytics_id: int
    current_party_id: int
    current_electoral_district_id: int
    name_full: str
    name_last: str
    name_first: str
    name_middle: str
    name_suffix: str
    gender: str
    candidate_image: str

    def __init__(self):
        self._goverlytics_id = None
        self._current_party_id = None
        self._current_electoral_district_id = None
        self._name_full = ''
        self._name_last = ''
        self._name_first = ''
        self._name_middle = ''
        self._name_suffix = ''
        self._gender = ''
        self._candidate_image = '' 

    @property
    def goverlytics_id(self) -> int:
        return self._goverlytics_id
    @goverlytics_id.setter
    def goverlytics_id(self, id: int) -> None:
        if not isinstance(id, int):
            raise TypeError("goverlytics_id must be an int")
        self._goverlytics_id = id

    @property
    def current_party_id(self) -> int:
        return self._current_party_id
    @current_party_id.setter
    def current_party_id(self, id: int) -> None:
        if not isinstance(id, int):
            raise TypeError("current_party_id must be an int")
        self._current_party_id = id

    @property
    def current_electoral_district_id(self) -> int:
        return self._current_electoral_district_id
    @current_electoral_district_id.setter
    def current_electoral_district_id(self, id: int) -> None:
        if not isinstance(id, int):
            raise TypeError("current_electoral_district_id must be an int")
        self._current_electoral_district_id = id

    @property
    def name_full(self) -> str:
        return self._name_full
    @name_full.setter
    def name_full(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("name_full must be a str")
        self._name_full = value

    @property
    def name_last(self) -> str:
        return self._name_last
    @name_last.setter
    def name_last(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("name_last must be a str")
        self._name_last = value

    @property
    def name_first(self) -> str:
        return self._name_first
    @name_first.setter
    def name_first(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("name_first must be a str")
        self._name_first = value

    @property
    def name_middle(self) -> str:
        return self._name_middle
    @name_middle.setter
    def name_middle(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("name_middle must be a str")
        self._name_middle = value

    @property
    def name_suffix(self) -> str:
        return self._name_suffix
    @name_suffix.setter
    def name_suffix(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("name_suffix must be a str")
        self._name_suffix = value

    @property
    def gender(self) -> str:
        return self._gender
    @gender.setter
    def gender(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("gender must be a str")
        possible_genders = ['M', 'F', 'O', '']
        if value not in possible_genders:
            raise ValueError('gender must be one of M (Male), F (Female), O (Other), or Blank')
        self._gender = value

    @property
    def candidate_image(self) -> str:
        return self._candidate_image
    @candidate_image.setter
    def candidate_image(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("candidate_image must be a str")
        self._candidate_image = value

@dataclass
class ElectionVotesRow:
    """
    Data structure for housing data for election votes
    """
    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    province_territory_id: int
    election_id: int
    ordinary_stationary: int
    ordinary_mobile: int
    advanced_polling: int
    special_voting_rules: int
    invalid_votes: int
    voter_turnout: int
    total: int


    def __init__(self):
        self._province_territory_id = None
        self._election_id = None
        self._ordinary_stationary = None
        self._ordinary_mobile = None
        self._advanced_polling = None
        self._special_voting_rules = None
        self._invalid_votes = None
        self._voter_turnout = None
        self._total = None

    @property
    def province_territory_id(self) -> int:
        return self._province_territory_id

    @province_territory_id.setter
    def province_territory_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("province_territory_id must be an int")
        self._province_territory_id = value

    @property
    def election_id(self) -> int:
        return self._election_id

    @election_id.setter
    def election_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("election_id must be an int")
        self._election_id = value

    @property
    def ordinary_stationary(self) -> int:
        return self._ordinary_stationary

    @ordinary_stationary.setter
    def ordinary_stationary(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("ordinary_stationary must be an int")
        self._ordinary_stationary = value

    @property
    def ordinary_mobile(self) -> int:
        return self._ordinary_mobile

    @ordinary_mobile.setter
    def ordinary_mobile(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("ordinary_mobile must be an int")
        self._ordinary_mobile = value

    @property
    def advanced_polling(self) -> int:
        return self._advanced_polling

    @advanced_polling.setter
    def advanced_polling(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("advanced_polling must be an int")
        self._advanced_polling = value

    @property
    def special_voting_rules(self) -> int:
        return self._special_voting_rules

    @special_voting_rules.setter
    def special_voting_rules(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("special_voting_rules must be an int")
        self._special_voting_rules = value


    @property
    def invalid_votes(self) -> int:
        return self._invalid_votes

    @invalid_votes.setter
    def invalid_votes(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("invalid_votes must be an int")
        self._invalid_votes = value


    @property
    def voter_turnout(self) -> int:
        return self._voter_turnout

    @voter_turnout.setter
    def voter_turnout(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("voter_turnout must be an int")
        self._voter_turnout = value


    @property
    def total(self) -> int:
        return self._total

    @total.setter
    def total(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("total must be an int")
        self._total = value
    

@dataclass
class CandidateElectionDetailsRow:
    """
    Data structure for housing data for candidate election details since 1867
    """
    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    candidate_id: int
    electoral_district_id: int
    party_id: int
    election_id: int
    is_incumbent: bool

    def __init__(self):
        self._candidate_id = None
        self._electoral_district_id = None
        self._party_id = None
        self._election_id = None
        self._is_incumbent = False

    @property
    def candidate_id(self) -> int:
        return self._candidate_id
    @candidate_id.setter
    def candidate_id(self, id: int) -> None:
        if not isinstance(id, int):
            raise TypeError("candidate_id must be an int")
        self._candidate_id = id

    @property
    def electoral_district_id(self) -> int:
        return self._electoral_district_id
    @electoral_district_id.setter
    def electoral_district_id(self, id: int) -> None:
        if not isinstance(id, int):
            raise TypeError("electoral_district_id must be an int")
        self._electoral_district_id = id

    @property
    def party_id(self) -> int:
        return self._party_id
    @party_id.setter
    def party_id(self, id: int) -> None:
        if not isinstance(id, int):
            raise TypeError("party_id must be an int")
        self._party_id = id


    @property
    def election_id(self) -> int:
        return self._election_id
    @election_id.setter
    def election_id(self, id: int) -> None:
        if not isinstance(id, int):
            raise TypeError("election_id must be an int")
        self._election_id = id

    @property
    def is_incumbent(self) -> bool:
        return self._is_incumbent
    @is_incumbent.setter
    def is_incumbent(self, value: bool) -> None:
        if type(value) != bool:
            raise TypeError("is_incumbent must be a bool")
        self._is_incumbent = value


@dataclass
class FinancialContributionsRow:
    """
    Data structure for housing data for financial contributions
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    recipient_id: int
    recipient_party_id: int
    recipient_name: str
    contributor_prov_terr_id: int
    contributor_name: str
    contributor_city: str
    contributor_postal_code: str
    date_received: str
    fiscal_year_or_event_date: str
    part_no_of_return: str
    contribution_type: str
    monetary_amount: float
    non_monetary_amount: float

    def __init__(self):
        self._recipient_id = None
        self._recipient_party_id = None
        self._recipient_name = ''
        self._contributor_prov_terr_id = None
        self._contributor_name = ''
        self._contributor_city = ''
        self._contributor_postal_code = ''
        self._date_received = ''
        self._fiscal_year_or_event_date = ''
        self._part_no_of_return = ''
        self._contribution_type = ''
        self._monetary_amount = None
        self._non_monetary_amount = None

    @property
    def recipient_id(self) -> int:
        return self._recipient_id

    @recipient_id.setter
    def recipient_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("recipient_id must be an int")
        self._recipient_id = value

    @property
    def recipient_party_id(self) -> int:
        return self._recipient_party_id

    @recipient_party_id.setter
    def recipient_party_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("recipient_party_id must be an int")
        self._recipient_party_id = value

    @property
    def recipient_name(self) -> str:
        return self._recipient_name

    @recipient_name.setter
    def recipient_name(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("recipient_name must be an string")
        self._recipient_name = value

    @property
    def contributor_prov_terr_id(self) -> int:
        return self._contributor_prov_terr_id

    @contributor_prov_terr_id.setter
    def contributor_prov_terr_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("contributor_prov_terr_id must be an int")
        self._contributor_prov_terr_id = value

    @property
    def contributor_name(self) -> str:
        return self._contributor_name

    @contributor_name.setter
    def contributor_name(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("contributor_name must be an string")
        self._contributor_name = value

    @property
    def contributor_city(self) -> str:
        return self._contributor_city

    @contributor_city.setter
    def contributor_city(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("contributor_city must be an string")
        self._contributor_city = value

    @property
    def contributor_postal_code(self) -> str:
        return self._contributor_postal_code

    @contributor_postal_code.setter
    def contributor_postal_code(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("contributor_postal_code must be an string")
        self._contributor_postal_code = value

    @property
    def date_received(self) -> str:
        return self._date_received

    @date_received.setter
    def date_received(self, value: str) -> None:
        if value and (not isinstance(value, str)):
            raise TypeError("election_date must be a str")
        if value and not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
            raise ValueError("Improper date formating in date_received. Required format: YYYY-MM-DD")
        self._date_received = value

    @property
    def fiscal_year_or_event_date(self) -> str:
        return self._fiscal_year_or_event_date

    @fiscal_year_or_event_date.setter
    def fiscal_year_or_event_date(self, value: str) -> None:
        if (not isinstance(value, str)) and value != None:
            raise TypeError("election_date must be a str")
        if value and not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
            raise ValueError("Improper date formating in fiscal_year_or_event_date. Required format: YYYY-MM-DD")
        self._fiscal_year_or_event_date = value

    @property
    def part_no_of_return(self) -> str:
        return self._part_no_of_return

    @part_no_of_return.setter
    def part_no_of_return(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("part_no_of_return must be an str")
        self._part_no_of_return = value

    @property
    def contribution_type(self) -> str:
        return self._contribution_type

    @contribution_type.setter
    def contribution_type(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("contribution_type must be an str")
        self._contribution_type = value

    @property
    def monetary_amount(self) -> float:
        return self._monetary_amount

    @monetary_amount.setter
    def monetary_amount(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("monetary_amount must be an str")
        self._monetary_amount = value

    @property
    def non_monetary_amount(self) -> float:
        return self._non_monetary_amount

    @non_monetary_amount.setter
    def non_monetary_amount(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("non_monetary_amount must be an str")
        self._non_monetary_amount = value

@dataclass
class CandidateElectionFinancesRow:
    """
    Data structure for housing data for candidate election finances
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    candidate_election_id: int
    date_of_return: str

    def __init__(self):
        self._candidate_election_id = None
        self._date_of_return = None

    @property
    def candidate_election_id(self) -> int:
        return self._candidate_election_id

    @candidate_election_id.setter
    def candidate_election_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("candidate_election_id must be an int")
        self._candidate_election_id = value

    @property
    def date_of_return(self) -> str:
        return self._date_of_return

    @date_of_return.setter
    def date_of_return(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("date_of_return must be an str")
        self._date_of_return = value

@dataclass
class InflowsRow:
    """
    Data structure for housing data for inflows
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    candidate_election_finances_id: int
    monetary: float
    non_monetary: float
    contribution_detail: dict
    contribution_totals: dict
    loans: float
    loans_received: dict
    loans_detail: List[dict]
    monetary_returned: float
    non_monetary_returned: float
    returned_detail: List[dict]
    monetary_transfer_received: float
    non_monetary_transfer_received: float
    transfer_totals: dict
    transfer_detail: List[dict]
    other_cash_inflow: float
    other_inflow_detail: List[dict]
    total_inflow: float

    def __init__(self):
        self._candidate_election_finances_id = None
        self._monetary = 0
        self._non_monetary = 0
        self._contribution_detail = {}
        self._contribution_totals = {}
        self._loans = 0
        self._loans_received = {}
        self._loans_detail = []
        self._monetary_returned = 0
        self._non_monetary_returned = 0
        self._returned_detail = []
        self._monetary_transfer_received = 0
        self._non_monetary_transfer_received = 0
        self._transfer_totals = {}
        self._transfer_detail = []
        self._other_cash_inflow = 0
        self._other_inflow_detail = []
        self._total_inflow = 0

    @property
    def candidate_election_finances_id(self) -> int:
        return self._candidate_election_finances_id

    @candidate_election_finances_id.setter
    def candidate_election_finances_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("candidate_election_finances_id must be an int")
        self._candidate_election_finances_id = value

    @property
    def monetary(self) -> float:
        return self._monetary

    @monetary.setter
    def monetary(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("monetary must be a float")
        self._monetary = value

    @property
    def non_monetary(self) -> float:
        return self._non_monetary

    @non_monetary.setter
    def non_monetary(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("non_monetary must be a float")
        self._non_monetary = value

    @property
    def contribution_detail(self) -> dict:
        return self._contribution_detail

    @contribution_detail.setter
    def contribution_detail(self, value: dict) -> None:
        if not isinstance(value, dict):
            raise TypeError("contribution_detail must be a dict")
        self._contribution_detail = value

    @property
    def contribution_totals(self) -> dict:
        return self._contribution_totals

    @contribution_totals.setter
    def contribution_totals(self, value: dict) -> None:
        if not isinstance(value, dict):
            raise TypeError("contribution_totals must be a dict")
        self._contribution_totals = value

    @property
    def loans(self) -> float:
        return self._loans

    @loans.setter
    def loans(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("loans must be a float")
        self._loans = value

    @property
    def loans_received(self) -> dict:
        return self._loans_received

    @loans_received.setter
    def loans_received(self, value: dict) -> None:
        if not isinstance(value, dict):
            raise TypeError("loans_received must be a dict")
        self._loans_received = value

    @property
    def loans_detail(self) -> List[dict]:
        return self._contribution_detail

    @loans_detail.setter
    def loans_detail(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("loans_detail be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("loans_detail must a list of dicts")
        self._loans_detail = value

    @property
    def monetary_returned(self) -> float:
        return self._monetary_returned

    @monetary_returned.setter
    def monetary_returned(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("monetary_returned must be a float")
        self._monetary_returned = value

    @property
    def non_monetary_returned(self) -> float:
        return self._non_monetary_returned

    @non_monetary_returned.setter
    def non_monetary_returned(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("non_monetary_returned must be a float")
        self._non_monetary_returned = value

    @property
    def returned_detail(self) -> List[dict]:
        return self._returned_detail

    @returned_detail.setter
    def returned_detail(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("returned_detail be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("returned_detail must a list of dicts")
        self._returned_detail = value

    @property
    def monetary_transfer_received(self) -> float:
        return self._monetary_transfer_received

    @monetary_transfer_received.setter
    def monetary_transfer_received(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("monetary_transfer_received must be a float")
        self._monetary_transfer_received = value

    @property
    def non_monetary_transfer_received(self) -> float:
        return self._non_monetary_transfer_received

    @non_monetary_transfer_received.setter
    def non_monetary_transfer_received(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("non_monetary_transfer_received must be a float")
        self._non_monetary_transfer_received = value

    @property
    def transfer_totals(self) -> dict:
        return self._transfer_totals

    @transfer_totals.setter
    def transfer_totals(self, value: dict) -> None:
        if not isinstance(value, dict):
            raise TypeError("transfer_totals must be a dict")
        self._transfer_totals = value

    @property
    def transfer_detail(self) -> List[dict]:
        return self._transfer_detail

    @transfer_detail.setter
    def transfer_detail(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("transfer_detail be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("transfer_detail must a list of dicts")
        self._transfer_detail = value

    @property
    def other_cash_inflow(self) -> float:
        return self._other_cash_inflow

    @other_cash_inflow.setter
    def other_cash_inflow(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("other_cash_inflow must be a float")
        self._other_cash_inflow = value

    @property
    def other_inflow_detail(self) -> List[dict]:
        return self._other_inflow_detail

    @other_inflow_detail.setter
    def other_inflow_detail(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("other_inflow_detail be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("other_inflow_detail must a list of dicts")
        self._other_inflow_detail = value

    @property
    def total_inflow(self) -> float:
        return self._total_inflow

    @total_inflow.setter
    def total_inflow(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("total_inflow must be a float")
        self._total_inflow = value

@dataclass
class CandidateElectionVotesRow:
    """
    Data structure for housing data for candidate election votes
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    candidate_election_id: int
    votes_obtained: int
    votes_percentage: float
    majority: int
    majority_percentage: float

    def __init__(self):
        self._candidate_election_id = None
        self._votes_obtained = None
        self._votes_percentage = None
        self._majority = None
        self._majority_percentage = None

    @property
    def candidate_election_id(self) -> int:
        return self._candidate_election_id

    @candidate_election_id.setter
    def candidate_election_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("candidate_election_id must be an int")
        self._candidate_election_id = value

    @property
    def votes_obtained(self) -> int:
        return self._votes_obtained

    @votes_obtained.setter
    def votes_obtained(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("votes obtained must be an int")
        self._votes_obtained = value

    @property
    def votes_percentage(self) -> float:
        return self._votes_percentage

    @votes_percentage.setter
    def votes_percentage(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("votes percentage must be an float")
        self._votes_percentage = value

    @property
    def majority(self) -> int:
        return self._majority

    @majority.setter
    def majority(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("majority must be an int")
        self._majority = value

    @property
    def majority_percentage(self) -> float:
        return self._majority_percentage

    @majority_percentage.setter
    def majority_percentage(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("majority_percentage must be an float")
        self._majority_percentage = value

@dataclass
class OutflowsRow:
    """
    Data structure for housing data for outflows
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    candidate_election_finances_id: int
    expenses_limit: float
    total_expenses_subject_to_limit: float
    total_expenses_subject_to_limit_detail: List[dict]
    personal_expenses: float
    personal_expenses_detail: List[dict]
    other_expenses: float
    other_detail: List[dict]
    campaign_expenses: float
    contributed_transferred_property_or_service: float
    non_monetary_transfers_sent_to_political_entities: List[dict]
    unpaid_claims: float
    unpaid_claims_detail: List[dict]
    total_outflows: float

    def __init__(self):
        self._candidate_election_finances_id = None
        self._expenses_limit = 0
        self._total_expenses_subject_to_limit = 0
        self._total_expenses_subject_to_limit_detail = []
        self._personal_expenses = 0
        self._personal_expenses_detail = []
        self._other_expenses = 0
        self._other_detail = []
        self._campaign_expenses = 0
        self._contributed_transferred_property_or_service = 0
        self._non_monetary_transfers_sent_to_political_entities = []
        self._unpaid_claims = 0
        self._unpaid_claims_detail = []
        self._total_outflows = 0

    @property
    def candidate_election_finances_id(self) -> int:
        return self._candidate_election_finances_id

    @candidate_election_finances_id.setter
    def candidate_election_finances_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("candidate_election_finances_id must be an int")
        self._candidate_election_finances_id = value

    @property
    def expenses_limit(self) -> float:
        return self._expenses_limit

    @expenses_limit.setter
    def expenses_limit(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("expenses_limit must be a float")
        self._expenses_limit = value

    @property
    def total_expenses_subject_to_limit(self) -> float:
        return self._total_expenses_subject_to_limit

    @total_expenses_subject_to_limit.setter
    def total_expenses_subject_to_limit(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("total_expenses_subject_to_limit must be a float")
        self._total_expenses_subject_to_limit = value

    @property
    def total_expenses_subject_to_limit_detail(self) -> List[dict]:
        return self._total_expenses_subject_to_limit_detail

    @total_expenses_subject_to_limit_detail.setter
    def total_expenses_subject_to_limit_detail(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("total_expenses_subject_to_limit_detail be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("total_expenses_subject_to_limit_detail must a list of dicts")
        self._total_expenses_subject_to_limit_detail = value

    @property
    def personal_expenses(self) -> float:
        return self._personal_expenses

    @personal_expenses.setter
    def personal_expenses(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("personal_expenses must be a float")
        self._personal_expenses = value

    @property
    def personal_expenses_detail(self) -> List[dict]:
        return self._personal_expenses_detail

    @personal_expenses_detail.setter
    def personal_expenses_detail(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("personal_expenses_detail be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("personal_expenses_detail must a list of dicts")
        self._personal_expenses_detail = value

    @property
    def other_expenses(self) -> float:
        return self._other_expenses

    @other_expenses.setter
    def other_expenses(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("other_expenses must be a float")
        self._other_expenses = value

    @property
    def other_detail(self) -> List[dict]:
        return self._other_detail

    @other_detail.setter
    def other_detail(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("other_detail be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("other_detail must a list of dicts")
        self._other_detail = value

    @property
    def campaign_expenses(self) -> float:
        return self._campaign_expenses

    @campaign_expenses.setter
    def campaign_expenses(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("campaign_expenses must be a float")
        self._campaign_expenses = value

    @property
    def contributed_transferred_property_or_service(self) -> float:
        return self._contributed_transferred_property_or_service

    @contributed_transferred_property_or_service.setter
    def contributed_transferred_property_or_service(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("contributed_transferred_property_or_service must be a float")
        self._contributed_transferred_property_or_service = value

    @property
    def non_monetary_transfers_sent_to_political_entities(self) -> List[dict]:
        return self._non_monetary_transfers_sent_to_political_entities

    @non_monetary_transfers_sent_to_political_entities.setter
    def non_monetary_transfers_sent_to_political_entities(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("non_monetary_transfers_sent_to_political_entities be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("non_monetary_transfers_sent_to_political_entities must a list of dicts")
        self._non_monetary_transfers_sent_to_political_entities = value

    @property
    def unpaid_claims(self) -> float:
        return self._unpaid_claims

    @unpaid_claims.setter
    def unpaid_claims(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("unpaid_claims must be a float")
        self._unpaid_claims = value

    @property
    def unpaid_claims_detail(self) -> List[dict]:
        return self._unpaid_claims_detail

    @unpaid_claims_detail.setter
    def unpaid_claims_detail(self, value: List[dict]) -> None:
        if type(value) != list:
            raise TypeError("unpaid_claims_detail be a list of dicts")
        for element in value:
            if type(element) != dict:
                raise TypeError("unpaid_claims_detail must a list of dicts")
        self._unpaid_claims_detail = value

    @property
    def total_outflows(self) -> float:
        return self._total_outflows

    @total_outflows.setter
    def total_outflows(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("total_outflows must be a float")
        self._total_outflows = value


@dataclass
class BankReconciliationRow:
    """
    Data structure for housing data for bank reconciliations
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    candidate_election_finances_id: int
    inflow: float
    outflow: float
    surplus: float

    def __init__(self):
        self._candidate_election_finances_id = None
        self._inflow = None
        self._outflow = None
        self._surplus = None


    @property
    def candidate_election_finances_id(self) -> int:
        return self._candidate_election_finances_id

    @candidate_election_finances_id.setter
    def candidate_election_finances_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("candidate_election_finances_id must be an int")
        self._candidate_election_finances_id = value

    @property
    def inflow(self) -> float:
        return self._inflow

    @inflow.setter
    def inflow(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("inflow must be an int")
        self._inflow = value

    @property
    def outflow(self) -> float:
        return self._outflow

    @outflow.setter
    def outflow(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("outflow must be an float")
        self._outflow = value

    @property
    def surplus(self) -> float:
        return self._surplus

    @surplus.setter
    def surplus(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("surplus must be an int")
        self._surplus = value


@dataclass
class BankAccountRow:
    """
    Data structure for housing data for bank accounts
    """

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    candidate_election_finances_id: int
    total_credits: float
    total_debits: float
    total_balance: float
    outstanding_cheques: float
    deposits_in_transit: float
    account_balance: float

    def __init__(self):
        self._candidate_election_finances_id = None
        self._total_credits = None
        self._total_debits = None
        self._total_balance = None
        self._outstanding_cheques = None
        self._deposits_in_transit = None
        self._account_balance = None

    @property
    def candidate_election_finances_id(self) -> int:
        return self._candidate_election_finances_id

    @candidate_election_finances_id.setter
    def candidate_election_finances_id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("candidate_election_finances_id must be an int")
        self._candidate_election_finances_id = value

    @property
    def total_credits(self) -> float:
        return self._total_credits

    @total_credits.setter
    def total_credits(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("total_credits must be an float")
        self._total_credits = value

    @property
    def total_debits(self) -> float:
        return self._total_debits

    @total_debits.setter
    def total_debits(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("total_debits must be an float")
        self._total_debits = value

    @property
    def total_balance(self) -> float:
        return self._total_balance

    @total_balance.setter
    def total_balance(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("total_balance must be an float")
        self._total_balance = value

    @property
    def outstanding_cheques(self) -> float:
        return self._outstanding_cheques

    @outstanding_cheques.setter
    def outstanding_cheques(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("outstanding_cheques must be an float")
        self._outstanding_cheques = value

    @property
    def deposits_in_transit(self) -> float:
        return self._deposits_in_transit

    @deposits_in_transit.setter
    def deposits_in_transit(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("deposits_in_transit must be an float")
        self._deposits_in_transit = value

    @property
    def account_balance(self) -> float:
        return self._account_balance

    @account_balance.setter
    def account_balance(self, value: float) -> None:
        if not isinstance(value, float):
            raise TypeError("account_balance must be an float")
        self._account_balance = value

