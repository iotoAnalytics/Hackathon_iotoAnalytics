from typing import List
from dataclasses import dataclass, field
from datetime import datetime


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
    goverlytics_id: int = None
    source_id: str = ''
    bill_name: str = ''
    session: str = ''
    date_introduced: datetime = None
    source_url: str = ''
    chamber_origin: str = ''
    committees: List[dict] = field(default_factory=list)
    bill_type: str =  ''
    bill_title: str = ''
    country_id: int = 0
    country: str = ''
    current_status: str = ''
    principal_sponsor_id: int = None
    principal_sponsor: str = ''
    sponsors: List[str] = field(default_factory=list)
    sponsors_id: List[int] = field(default_factory=list)
    cosponsors: List[str] = field(default_factory=list)
    cosponsors_id: List[int] = field(default_factory=list)
    bill_text: str = ''
    bill_description: str =''
    bill_summary:str = ''
    actions: List[dict] = field(default_factory=list)
    votes: List[dict] = field(default_factory=list)
    source_topic: str = ''
    topic: str = ''

class USLegislationRow(LegislationRow):
    """
    Data structure for housing data about each piece of legislation.
    """
    state_id: int = 0
    state: str = ''

class CadLegislationRow(LegislationRow):
    """
    Data structure for housing data about each piece of legislation.
    """
    province_territory_id: int = 0
    province_territory: str = ''


class CadFedLegislationRow(CadLegislationRow):
    """
    Data structure for housing data about each piece of legislation.
    """
    sponsor_affiliation: str = ''
    sponsor_gender: str = ''
    pm_name_full: str = ''
    pm_party: str = ''
    pm_party_id: int = 0
    statute_year: int = 0
    statute_chapter: int = 0
    publications: List[str] = field(default_factory=list)
    last_major_event: dict = {}


#########################################################
#       LEGISLATOR ROWS                                 #
#########################################################
@dataclass
class LegislatorRow:
    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    most_recent_term_id: str = ''
    name_full: str = ''
    name_last: str = ''
    name_first: str = ''
    name_middle: str = ''
    name_suffix: str = ''
    country_id: int = None
    country: str = ''
    party_id: int = None
    party: str = ''
    role: str = ''
    years_active: List[int] = field(default_factory=list)
    committees: List[dict] = field(default_factory=list)
    phone_number: List[dict] = field(default_factory=list)
    addresses: List[dict] = field(default_factory=list)
    email: str = ''
    birthday: datetime = None
    seniority: int = 0
    occupation: List[str] = field(default_factory=list)
    education: List[dict] = field(default_factory=list)
    military_experience: str = ''
    source_url: str = ''
    source_id: str = ''


@dataclass
class USLegislatorRow(LegislatorRow):
    """
    Data structure for housing data about each piece of legislator.
    """
    state: str = ''
    state_id: int = None
    district: str = ''
    areas_served: List[str] = field(default_factory=list)


@dataclass
class CadLegislatorRow(LegislatorRow):
    """
    Data structure for housing data about each piece of legislator.
    """
    province_territory_id: int = None
    province_territory: str = ''
    riding: str = ''
    region: str = ''

@dataclass
class CadFedLegislatorRow(CadLegislatorRow):
    """
    Data structure for housing data about each piece of legislator.
    """
    offices_roles_as_mp: List[str] = field(default_factory=list)
    parl_assoc_interparl_groups: List[dict] = field(default_factory=list)